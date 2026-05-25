package gobroker

import (
	"time"
	"sync"
)

type BrokerCallback[T any] func() T

type GoBroker[T any] struct {
	mu sync.RWMutex
	cond *sync.Cond
	rcond *sync.Cond

	cache T
	intervalWait time.Duration
	intervalNext time.Time
	
	isUpdating bool
	hasUpdated bool

	wasAsked bool

	callback BrokerCallback[T] // Should be set only once
}

func NewGoBroker[T any](callback BrokerCallback[T], intervalDuration string) *GoBroker[T] {
	broker := &GoBroker[T]{}
	broker.cond = sync.NewCond( &broker.mu )
	broker.rcond = sync.NewCond( broker.mu.RLocker() )
	broker.intervalWait, _ = time.ParseDuration(intervalDuration)

	return broker
}

func (broker *GoBroker[T]) update() {
	broker.mu.Lock() // Lock 1
	broker.isUpdating = true
	defer broker.mu.Unlock() // Unlock 0

	for {
		if !broker.wasAsked {
			break
		}
		waitTime := time.Until( broker.intervalNext )
		broker.mu.Unlock() // Unlock 0

		time.Sleep( waitTime )
		result := broker.callback()

		broker.mu.Lock() // Lock 1
		broker.cache = result
		broker.intervalNext = time.Now().Add( broker.intervalWait )
		broker.hasUpdated = true
		broker.rcond.Signal() // Release one broker
	}
	
	broker.isUpdating = false
}

func (broker *GoBroker[T]) Ask(ahead bool, andWait bool) T {
	broker.mu.Lock() // Lock 1
	if ahead {
		broker.wasAsked = true
	}
	
	if !broker.isUpdating && (!broker.hasUpdated && time.Now().After(broker.intervalNext) || ahead) {
		broker.wasAsked = true
		go broker.update()
	}
	broker.mu.Unlock() // Unlock 0

	broker.mu.RLock() // RLock 1
	if andWait && !broker.hasUpdated {
		broker.rcond.Wait() // RUnlocks and RLocks : 1
	}

	if broker.hasUpdated {
		broker.mu.RUnlock() // RUnlock 0

		broker.mu.Lock() // Lock 1
		broker.hasUpdated = false
		broker.rcond.Broadcast() // Release the rest of the brokers
		broker.mu.Unlock() // Unlock 0

		broker.mu.RLock() // RLock 1
	}

	defer broker.mu.RUnlock() // RUnlock 0
	return broker.cache
}
