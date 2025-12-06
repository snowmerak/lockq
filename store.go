package lockq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

// Redis hash field names for task data.
const (
	taskFieldType        = "ty"
	taskFieldPayload     = "pl"
	taskFieldLockKey     = "lk"
	taskFieldRepeat      = "rp"
	taskFieldAttempts    = "at"
	taskFieldMaxAttempts = "ma"
	taskFieldExecutionID = "ex"
)

var ErrTaskExists = errors.New("identical repeating task type with same lock key already exists")

// Store handles Redis operations for task persistence and retrieval.
type Store struct {
	redis  *redis.Client
	config *Config
}

// NewStore creates a Store instance.
func NewStore(rdb *redis.Client, config *Config) (*Store, error) {
	config = config.WithDefaults()
	return &Store{
		redis:  rdb,
		config: config,
	}, nil
}

// Push adds a task to the appropriate queue based on its lock key.
func (s *Store) Push(ctx context.Context, task *Task) (string, error) {
	if task.ID == "" {
		task.ID = uuid.NewString()
	}

	delay := time.Until(task.ScheduledAt)
	if delay < 0 {
		delay = 0
	}

	if task.IsLocked() {
		return s.pushLocked(ctx, task, delay)
	}
	return s.pushUnlocked(ctx, task, delay)
}

func (s *Store) pushUnlocked(ctx context.Context, task *Task, delay time.Duration) (string, error) {
	queueKey := s.config.UnlockedQueueKeyFor(task.Type)
	taskKey := s.config.TaskKeyPrefix() + task.ID

	now := time.Now()
	score := now.UnixMicro() + delay.Microseconds()

	pipe := s.redis.TxPipeline()
	pipe.HSet(ctx, taskKey, map[string]interface{}{
		taskFieldType:        task.Type,
		taskFieldPayload:     task.Payload,
		taskFieldMaxAttempts: task.MaxAttempts,
	})
	if task.Repeat > 0 {
		pipe.HSet(ctx, taskKey, taskFieldRepeat, task.Repeat.Milliseconds())
	}
	pipe.ZAdd(ctx, queueKey, &redis.Z{Score: float64(score), Member: task.ID})

	_, err := pipe.Exec(ctx)
	if err != nil {
		return "", fmt.Errorf("push unlocked: %w", err)
	}

	return task.ID, nil
}

func (s *Store) pushLocked(ctx context.Context, task *Task, delay time.Duration) (string, error) {
	queueKey := s.config.LockedQueueKeyFor(task.Type)
	taskKey := s.config.TaskKeyPrefix() + task.ID

	now := time.Now()
	score := now.UnixMicro() + delay.Microseconds()

	// Check for duplicate repeating task
	if task.Repeat > 0 && task.LockKey != "" {
		repeatLockKey := s.config.RepeatLockKeyPrefix() + task.LockKey
		set, err := s.redis.SetNX(ctx, repeatLockKey, task.ID, 0).Result()
		if err != nil {
			return "", fmt.Errorf("check repeat lock: %w", err)
		}
		if !set {
			return "", ErrTaskExists
		}
	}

	pipe := s.redis.TxPipeline()
	pipe.HSet(ctx, taskKey, map[string]interface{}{
		taskFieldType:        task.Type,
		taskFieldPayload:     task.Payload,
		taskFieldLockKey:     task.LockKey,
		taskFieldMaxAttempts: task.MaxAttempts,
	})
	if task.Repeat > 0 {
		pipe.HSet(ctx, taskKey, taskFieldRepeat, task.Repeat.Milliseconds())
	}
	pipe.ZAdd(ctx, queueKey, &redis.Z{Score: float64(score), Member: task.ID})

	_, err := pipe.Exec(ctx)
	if err != nil {
		return "", fmt.Errorf("push locked: %w", err)
	}

	return task.ID, nil
}

// PopUnlocked retrieves and locks a task from the unlocked queue for the given task type.
func (s *Store) PopUnlocked(ctx context.Context, taskType string) (*Task, error) {
	queueKey := s.config.UnlockedQueueKeyFor(taskType)
	taskKeyPrefix := s.config.TaskKeyPrefix()

	now := time.Now()
	nowMicro := now.UnixMicro()

	// Try with PeekSize
	peekSize := s.config.PeekSize
	vals, err := s.redis.ZRangeByScoreWithScores(ctx, queueKey, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    strconv.FormatInt(nowMicro, 10),
		Offset: 0,
		Count:  int64(peekSize),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("zrangebyscore peek: %w", err)
	}

	// If not enough, try FallbackPeekSize
	if len(vals) == 0 && s.config.FallbackPeekSize > peekSize {
		vals, err = s.redis.ZRangeByScoreWithScores(ctx, queueKey, &redis.ZRangeBy{
			Min:    "-inf",
			Max:    strconv.FormatInt(nowMicro, 10),
			Offset: 0,
			Count:  int64(s.config.FallbackPeekSize),
		}).Result()
		if err != nil {
			return nil, fmt.Errorf("zrangebyscore fallback: %w", err)
		}
	}

	for _, z := range vals {
		taskID := z.Member.(string)
		taskKey := taskKeyPrefix + taskID
		claimKey := taskKey + ":claim"

		// Try to claim the task temporarily using SETNX
		claimed, err := s.redis.SetNX(ctx, claimKey, "claimed", 5*time.Second).Result()
		if err != nil || !claimed {
			continue
		}

		// Get task metadata
		fields, err := s.redis.HMGet(ctx, taskKey,
			taskFieldType, taskFieldPayload, taskFieldRepeat, taskFieldAttempts, taskFieldMaxAttempts).Result()
		if err != nil {
			s.redis.Del(ctx, claimKey)
			continue
		}

		// Check if task data exists
		if fields[0] == nil {
			s.redis.ZRem(ctx, queueKey, taskID)
			s.redis.Del(ctx, claimKey)
			continue
		}

		// Parse fields
		ty := toString(fields[0])
		pl := []byte(toString(fields[1]))
		rpStr := toString(fields[2])
		atStr := toString(fields[3])
		mrStr := toString(fields[4])

		repeatMs, _ := strconv.Atoi(rpStr)
		attempts, _ := strconv.Atoi(atStr)
		maxRetries, _ := strconv.Atoi(mrStr)

		execID := fmt.Sprintf("%d-%d-%s", now.Unix(), now.Nanosecond()/1000, taskID)

		// Update task in pipeline
		pipe := s.redis.TxPipeline()
		pipe.HSet(ctx, taskKey, taskFieldExecutionID, execID)
		pipe.HIncrBy(ctx, taskKey, taskFieldAttempts, 1)

		var zremCmd *redis.IntCmd
		if repeatMs > 0 {
			score := nowMicro + int64(repeatMs)*1000
			pipe.ZAdd(ctx, queueKey, &redis.Z{Score: float64(score), Member: taskID})
		} else {
			zremCmd = pipe.ZRem(ctx, queueKey, taskID)
			pipe.PExpire(ctx, taskKey, s.config.LockTimeout)
		}

		// Release claim lock
		pipe.Del(ctx, claimKey)

		_, err = pipe.Exec(ctx)
		if err != nil {
			continue
		}

		// If one-off task, verify we actually removed it
		if repeatMs == 0 && zremCmd != nil && zremCmd.Val() == 0 {
			continue
		}

		return &Task{
			ID:          taskID,
			Type:        ty,
			Payload:     pl,
			LockKey:     "",
			Repeat:      time.Duration(repeatMs) * time.Millisecond,
			Attempts:    attempts,
			MaxAttempts: maxRetries,
			ExecutionID: execID,
		}, nil
	}

	return nil, nil
}

// PopLocked retrieves and locks a task from the locked queue for the given task type
// if its lock is available.
func (s *Store) PopLocked(ctx context.Context, taskType string) (*Task, error) {
	queueKey := s.config.LockedQueueKeyFor(taskType)

	now := time.Now()
	nowMicro := now.UnixMicro()

	// Try with PeekSize
	peekSize := s.config.PeekSize
	vals, err := s.redis.ZRangeByScoreWithScores(ctx, queueKey, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    strconv.FormatInt(nowMicro, 10),
		Offset: 0,
		Count:  int64(peekSize),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("zrangebyscore peek: %w", err)
	}

	// If not enough, try FallbackPeekSize
	if len(vals) == 0 && s.config.FallbackPeekSize > peekSize {
		vals, err = s.redis.ZRangeByScoreWithScores(ctx, queueKey, &redis.ZRangeBy{
			Min:    "-inf",
			Max:    strconv.FormatInt(nowMicro, 10),
			Offset: 0,
			Count:  int64(s.config.FallbackPeekSize),
		}).Result()
		if err != nil {
			return nil, fmt.Errorf("zrangebyscore fallback: %w", err)
		}
	}

	for _, z := range vals {
		taskID := z.Member.(string)
		taskKey := s.config.TaskKeyPrefix() + taskID

		// Get task metadata
		fields, err := s.redis.HMGet(ctx, taskKey,
			taskFieldType, taskFieldPayload, taskFieldLockKey, taskFieldRepeat, taskFieldAttempts, taskFieldMaxAttempts).Result()
		if err != nil {
			continue
		}

		lockKeyStr := toString(fields[2])
		if lockKeyStr == "" {
			// No lock needed, remove invalid task
			s.redis.ZRem(ctx, queueKey, taskID)
			s.redis.Del(ctx, taskKey)
			continue
		}

		lockKey := s.config.LockKeyPrefix() + lockKeyStr
		execID := fmt.Sprintf("%d-%d-%s", now.Unix(), now.Nanosecond()/1000, taskID)

		// Try to acquire lock
		set, err := s.redis.SetNX(ctx, lockKey, execID, s.config.LockTimeout).Result()
		if err != nil || !set {
			continue
		}

		// Parse fields
		ty := toString(fields[0])
		pl := []byte(toString(fields[1]))
		rpStr := toString(fields[3])
		atStr := toString(fields[4])
		mrStr := toString(fields[5])

		repeatMs, _ := strconv.Atoi(rpStr)
		attempts, _ := strconv.Atoi(atStr)
		maxRetries, _ := strconv.Atoi(mrStr)

		// Update task in transaction
		pipe := s.redis.TxPipeline()
		pipe.HSet(ctx, taskKey, taskFieldExecutionID, execID)
		pipe.HIncrBy(ctx, taskKey, taskFieldAttempts, 1)
		if repeatMs > 0 {
			score := nowMicro + int64(repeatMs)*1000
			pipe.ZAdd(ctx, queueKey, &redis.Z{Score: float64(score), Member: taskID})
		} else {
			pipe.ZRem(ctx, queueKey, taskID)
			pipe.PExpire(ctx, taskKey, s.config.LockTimeout)
		}

		_, err = pipe.Exec(ctx)
		if err != nil {
			// Release lock on failure
			s.redis.Del(ctx, lockKey)
			continue
		}

		return &Task{
			ID:          taskID,
			Type:        ty,
			Payload:     pl,
			LockKey:     lockKeyStr,
			Repeat:      time.Duration(repeatMs) * time.Millisecond,
			Attempts:    attempts,
			MaxAttempts: maxRetries,
			ExecutionID: execID,
		}, nil
	}

	return nil, nil
}

func (s *Store) parseTaskResult(result interface{}) (*Task, error) {
	data, ok := result.([]interface{})
	if !ok || len(data) < 8 {
		return nil, fmt.Errorf("invalid task result format")
	}

	task := &Task{
		ID:          toString(data[0]),
		Type:        toString(data[1]),
		Payload:     []byte(toString(data[2])),
		LockKey:     toString(data[3]),
		Attempts:    int(toInt64(data[5])),
		MaxAttempts: int(toInt64(data[6])),
		ExecutionID: toString(data[7]),
	}

	if repeatMs := toInt64(data[4]); repeatMs > 0 {
		task.Repeat = time.Duration(repeatMs) * time.Millisecond
	}

	return task, nil
}

// Unlock cleans up task state after processing.
func (s *Store) Unlock(ctx context.Context, task *Task) error {
	taskKey := s.config.TaskKeyPrefix() + task.ID

	fields, err := s.redis.HMGet(ctx, taskKey, taskFieldRepeat, taskFieldLockKey).Result()
	if err != nil {
		return fmt.Errorf("hmget unlock: %w", err)
	}

	rpStr := toString(fields[0])
	lkStr := toString(fields[1])

	repeatMs, _ := strconv.Atoi(rpStr)

	if repeatMs == 0 {
		s.redis.Del(ctx, taskKey)
	}

	if lkStr != "" {
		lockKey := s.config.LockKeyPrefix() + lkStr
		if val, _ := s.redis.Get(ctx, lockKey).Result(); val == task.ExecutionID {
			s.redis.Del(ctx, lockKey)
		}
	}

	return nil
}

// ReleaseLock releases a distributed lock if owned by the current execution.
func (s *Store) ReleaseLock(ctx context.Context, task *Task) error {
	if task.LockKey == "" {
		return nil
	}

	lockKey := s.config.LockKeyPrefix() + task.LockKey

	if val, err := s.redis.Get(ctx, lockKey).Result(); err == nil && val == task.ExecutionID {
		return s.redis.Del(ctx, lockKey).Err()
	}
	return nil
}

// Requeue returns a task to its queue for retry after the specified delay.
func (s *Store) Requeue(ctx context.Context, task *Task, delay time.Duration) error {
	var queueKey string
	if task.IsLocked() {
		queueKey = s.config.LockedQueueKeyFor(task.Type)
	} else {
		queueKey = s.config.UnlockedQueueKeyFor(task.Type)
	}

	taskKey := s.config.TaskKeyPrefix() + task.ID

	scoreUs := time.Now().Add(delay).UnixMicro()

	pipe := s.redis.TxPipeline()
	pipe.Persist(ctx, taskKey)
	pipe.ZAdd(ctx, queueKey, &redis.Z{Score: float64(scoreUs), Member: task.ID})
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("requeue task: %w", err)
	}

	return nil
}

// MoveToDeadLetter moves a failed task to the dead letter queue.
func (s *Store) MoveToDeadLetter(ctx context.Context, task *Task, taskErr error) error {
	taskKey := s.config.TaskKeyPrefix() + task.ID

	var queueKey string
	if task.IsLocked() {
		queueKey = s.config.LockedQueueKeyFor(task.Type)
	} else {
		queueKey = s.config.UnlockedQueueKeyFor(task.Type)
	}

	failedAt := time.Now().Unix()

	pipe := s.redis.TxPipeline()
	pipe.Persist(ctx, taskKey)
	pipe.HSet(ctx, taskKey, "error", taskErr.Error(), "failed_at", failedAt)
	pipe.ZAdd(ctx, s.config.DeadLetterQueueKey(), &redis.Z{Score: float64(failedAt), Member: task.ID})

	if task.IsRepeat() {
		pipe.ZRem(ctx, queueKey, task.ID)
		if task.IsLocked() {
			lockKey := s.config.LockKeyPrefix() + task.LockKey
			if val, _ := s.redis.Get(ctx, lockKey).Result(); val == task.ExecutionID {
				pipe.Del(ctx, lockKey)
			}
			// Also release the repeat lock so a new task with the same lock key can be enqueued
			repeatLockKey := s.config.RepeatLockKeyPrefix() + task.LockKey
			pipe.Del(ctx, repeatLockKey)
		}
	}

	if task.IsLocked() {
		lockKey := s.config.LockKeyPrefix() + task.LockKey
		if val, _ := s.redis.Get(ctx, lockKey).Result(); val == task.ExecutionID {
			pipe.Del(ctx, lockKey)
		}
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("move to dlq: %w", err)
	}

	return nil
}

// DeleteTask removes a task and its associated locks from Redis.
func (s *Store) DeleteTask(ctx context.Context, taskID string) error {
	taskKey := s.config.TaskKeyPrefix() + taskID

	ty, err := s.redis.HGet(ctx, taskKey, taskFieldType).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) || err.Error() == "redis: nil" {
			return nil
		}
		return fmt.Errorf("lookup task type for delete: %w", err)
	}

	lockedQueueKey := s.config.LockedQueueKeyFor(ty)
	unlockedQueueKey := s.config.UnlockedQueueKeyFor(ty)

	fields, err := s.redis.HMGet(ctx, taskKey, taskFieldLockKey, taskFieldRepeat).Result()
	if err != nil {
		return fmt.Errorf("hmget: %w", err)
	}

	lk := toString(fields[0])
	rp := toString(fields[1])

	pipe := s.redis.TxPipeline()
	pipe.ZRem(ctx, unlockedQueueKey, taskID)
	pipe.ZRem(ctx, lockedQueueKey, taskID)
	pipe.ZRem(ctx, s.config.DeadLetterQueueKey(), taskID)
	pipe.Del(ctx, taskKey)

	if lk != "" {
		lockKey := s.config.LockKeyPrefix() + lk
		pipe.Del(ctx, lockKey)
		if rp != "" {
			repeatLockKey := s.config.RepeatLockKeyPrefix() + lk
			pipe.Del(ctx, repeatLockKey)
		}
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("delete task: %w", err)
	}

	return nil
}

// Count returns the number of tasks ready within timeDelta from now.
func (s *Store) Count(ctx context.Context, queueKey string, timeDelta time.Duration) (int64, error) {
	now := time.Now().UnixMicro()
	max := now + timeDelta.Microseconds()
	count, err := s.redis.ZCount(ctx, queueKey, "-inf", strconv.FormatInt(max, 10)).Result()
	if err != nil {
		return 0, fmt.Errorf("zcount: %w", err)
	}
	return count, nil
}

// CountAll returns the total number of tasks in the queue regardless of scheduled time.
func (s *Store) CountAll(ctx context.Context, queueKey string) (int64, error) {
	return s.redis.ZCard(ctx, queueKey).Result()
}

// RetryFromDLQ moves a task from the dead letter queue back to processing.
func (s *Store) RetryFromDLQ(ctx context.Context, taskID string) error {
	taskKey := s.config.TaskKeyPrefix() + taskID

	exists, err := s.redis.Exists(ctx, taskKey).Result()
	if err != nil {
		return fmt.Errorf("exists: %w", err)
	}
	if exists == 0 {
		return fmt.Errorf("task %s not found", taskID)
	}

	ty, err := s.redis.HGet(ctx, taskKey, taskFieldType).Result()
	if err != nil {
		return fmt.Errorf("hget type: %w", err)
	}

	lockedQueueKey := s.config.LockedQueueKeyFor(ty)
	unlockedQueueKey := s.config.UnlockedQueueKeyFor(ty)

	fields, err := s.redis.HMGet(ctx, taskKey, taskFieldLockKey, taskFieldRepeat).Result()
	if err != nil {
		return fmt.Errorf("hmget: %w", err)
	}

	lk := toString(fields[0])
	rp := toString(fields[1])

	if rp != "" && lk != "" {
		repeatLockKey := s.config.RepeatLockKeyPrefix() + lk
		set, err := s.redis.SetNX(ctx, repeatLockKey, taskID, 0).Result()
		if err != nil {
			return fmt.Errorf("setnx repeat lock: %w", err)
		}
		if !set {
			return fmt.Errorf("repeat lock already exists for lock key")
		}
	}

	now := time.Now().UnixMicro()

	pipe := s.redis.TxPipeline()
	pipe.Persist(ctx, taskKey)
	pipe.HSet(ctx, taskKey, taskFieldAttempts, 0)
	pipe.HDel(ctx, taskKey, "error", "failed_at")
	pipe.ZRem(ctx, s.config.DeadLetterQueueKey(), taskID)

	if lk != "" {
		pipe.ZAdd(ctx, lockedQueueKey, &redis.Z{Score: float64(now), Member: taskID})
	} else {
		pipe.ZAdd(ctx, unlockedQueueKey, &redis.Z{Score: float64(now), Member: taskID})
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("retry from dlq: %w", err)
	}

	return nil
}

// ResetAttempts resets the attempts counter if executionID matches.
func (s *Store) ResetAttempts(ctx context.Context, taskID string, executionID string) (bool, error) {
	taskKey := s.config.TaskKeyPrefix() + taskID

	exists, err := s.redis.Exists(ctx, taskKey).Result()
	if err != nil {
		return false, fmt.Errorf("exists: %w", err)
	}
	if exists == 0 {
		return false, nil
	}

	currentExecID, err := s.redis.HGet(ctx, taskKey, taskFieldExecutionID).Result()
	if err != nil {
		return false, fmt.Errorf("hget exec id: %w", err)
	}
	if currentExecID != executionID {
		return false, nil
	}

	err = s.redis.HSet(ctx, taskKey, taskFieldAttempts, 0).Err()
	if err != nil {
		return false, fmt.Errorf("hset attempts: %w", err)
	}

	return true, nil
}

func toString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func toInt64(v interface{}) int64 {
	if i, ok := v.(int64); ok {
		return i
	}
	return 0
}
