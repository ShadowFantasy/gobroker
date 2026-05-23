package gobroker

// Imports

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

func Ask() {

}

type BrokerCallback[T any] func() T

type GoBroker[T any] struct {
	initial T
	cache T
	intervalRefresh int
	callback BrokerCallback[T]
}
