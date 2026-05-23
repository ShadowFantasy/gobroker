package gobroker

// Imports
import (
	"time"
	"sync"
)


// Ask(*gobroker)
// Asks a gobroker to get some information

// GoBroker
// The gobroker initiates with a cache that contains a defualt value
// When asked for information it will start a subprocess
// While the subprocess runs the gobroker will give back the cached value
// When the subprocess completes the gobroker will update it's cached value with the new data
// When GoBroker has new data it will enter a state that indicates it has new information
// GoBroker will also keep track of when the new information was obtained.
// Next time it is asked for information it will give the new cached value and then the new information state will change to old information
// When GoBroker is next asked for new information it give back the cached value and start searching for more information.
// The GoBroker can be assigned an intervalRefresh rate. If the GoBroker has finished a process and is still in an interval refresh rate it will wait till that refresh rate ends and start searching for new information
// While it is searching for new information or if it's during an interval where it is still refreshing it will continue to provide the cached value

type BrokerCallback[T any] func() T

type GoBroker[T any] struct {
	mu sync.Mutex
	cd sync.Cond

	cache T
	intervalRefreshDuration time.Duration
	intervalRefresh time.Time

	isUpdatingCache bool
	hasUpdatedCache bool
	wasAsked bool

	callback BrokerCallback[T]
}

func (broker *GoBroker[T any]) updateHelper() {
	broker.wasAsked = false
	waitTime := time.Until( broker.intervalRefresh )
		
	broker.mu.Unlock()

	time.Sleep( waitTime )
	result := broker.callback()

	broker.mu.Lock()
		
	broker.cache = result
	broker.intervalRefresh = time.Now().Add( broker.intervalRefreshDuration )
	broker.hasUpdatedCache = true

}

func (broker *GoBroker[T any]) update() {
	broker.mu.Lock()

	broker.isUpdatingCache = true

	broker.updateHelper()
	broker.wg.Done()

	for broker.wasAsked {
		broker.wg.Add(1)
		broker.updateHelper()
		broker.wg.Done()
	}
	broker.isUpdatingCache = false

	broker.mu.Unlock()
}

// Next Calls: Checks if GoBroker cache was updated
// If GoBroker cache has not updated (hasUpdatedCache is false) return cache value
// If GoBroker cache has updated (hasUpdatedCache is true) set hasUpdatedCache to false return cache
// If GoBroker has a refresh interval, continue to return the cache update without running an update
// If Asking ahead, queue GoBroker to get information immediatly after obtained new information or immediatly after refresh interval has finished
// If Asking and waiting, pause execuation until cache has been updated.
func (broker *GoBroker[T any]) Ask(ahead bool, andWait bool) T {
	broker.mu.Lock()
	
	if broker.hasUpdatedCache {
		andWait = false
	}

	if ahead {
		broker.wasAsked = true
		broker.hasUpdatedCache = false
	}

	if broker.hasUpdatedCache {
		broker.hasUpdatedCache = false
		defer broker.mu.Unlock()
		return broker.cache
	}

	if !broker.isUpdatingCache && time.Now().After( broker.intervalRefresh ) {
		broker.wasAsked = true

		broker.wg.Add(1)
		go broker.update()
	}

	if andWait {
		broker.mu.Unlock()

		broker.wg.Wait()

		broker.mu.Lock()

		broker.hasUpdatedCache = false
	}

	defer broker.mu.Unlock()
	return broker.cache
}
