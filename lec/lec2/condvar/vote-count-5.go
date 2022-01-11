package main

import "time"
import "math/rand"

func main() {
	rand.Seed(time.Now().UnixNano())

	count := 0
	finished := 0
	ch := make(chan bool)
	for i := 0; i < 10; i++ {
		// have another thread to do this
		go func() {
			ch <- requestVote()
		}()
	}
	for count < 5 && finished < 10 {
		// fetch channel after starting the thread
		// channel pops after both ends are ready
		v := <-ch
		if v {
			count += 1
		}
		finished += 1
	}
	// after count exceeds 5, all threads coming after are blocked by the channel, for no one is reading from this end.
	// leaking thread...
	if count >= 5 {
		println("received 5+ votes!")

	} else {
		println("lost")
	}
}

func requestVote() bool {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	return rand.Int()%2 == 0
}
