package pubysuby

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"runtime"
	"strconv"
	"testing"
	"time"
)
/*
func TestSub(t *testing.T) {
	t.Parallel()
	runtime.GOMAXPROCS(4)

	ps := NewPubySuby()

	subscription := ps.Sub("Test")
	doneReceivingMessages := make(chan int)
	go func() {
		// consumer
		var count int
		for messages := range subscription.ListenChannel {
			count = count + len(messages)
		}
		doneReceivingMessages <- count
	}()

	go func() {
		ps.Push("Test", "1")
		ps.Push("Test", "2")
		//fmt.Println("Start unsubscribe")
		ps.Unsubscribe(subscription)
		//fmt.Println("End unsubscribe")
		ps.Push("Test", "3")
		ps.Push("Test", "4")
		//close(in)
	}()
	count := <-doneReceivingMessages
	assert.Equal(t, count, 2)
	// wait for 1 second to see what happens when we push 3, 4 on the que
	// <-time.After(time.Second * 1)
	//	in, stop := ps.Sub("Test")
	//	go func() {
	//		var count int
	//		for val := range in {
	//			count = count + len(val)
	//		}
	//	}()
	//	<-time.After(time.Second * 1)
	//	close(stop)

	//	lastMessageId := ps.Push("Test", "one")
	//	assert.Equal(t, lastMessageId, ps.LastMessageId("Test"))
	//
	//	results := ps.PullSince("Test", 1, lastMessageId)
	//	if len(results) != 0 {
	//		t.Error("Expected 0 messages, got ", len(results))
	//	}
	//	ps.Push("Test", "two")
	//	results = ps.PullSince("Test", 1, lastMessageId)
	//	if len(results) != 1 {
	//		t.Error("Expected 0 messages, got ", len(results))
	//	}
	//	//ps.Push("Test", "three")
	//	messages := ps.Pull("Test", 0)
	//	if len(messages) != 2 {
	//		t.Error("Expected 2 messages, got ", len(messages))
	//	}
	//	ps.Push("Test", "three")

}

func TestUnsubscribeManyTimes(t *testing.T) {
	runtime.GOMAXPROCS(4)
	t.Parallel()
	//t.SkipNow()
	ps := NewPubySuby()

	subscription := ps.Sub("Test")
	var _ = subscription
	go func() {
		for {
			// ever 10 ms push a message on the topic
			<-time.After(time.Millisecond * 10)
			ps.Push("Test", "Hello")
		}

	}()
	go func() {
		for {
			select {
			case <-time.After(time.Second * 1):
				ps.Unsubscribe(subscription)
			case _, _ = <- subscription.ListenChannel:

			}
		}
	}()

	// lets create many subscriptions
	for i := 0; i < 100; i++ {
		sub := ps.Sub("Test")
		go func() {
			for _ = range sub.ListenChannel {

			}
		}()
		for i := 0; i < 100; i++ {
			ps.Unsubscribe(sub)
		}
	}
}
*/
func TestPull(t *testing.T) {
	runtime.GOMAXPROCS(4)
	t.Parallel()
	//t.SkipNow()
	ps := NewPubySuby()
	// generate 100 listeners
	// each one listens for 100 milliseconds
	for i := 0; i < 1000; i++ {
		go func() {
			messages := ps.Pull("TestPull", 100)
			if len(messages) == 0 {
				//t.Error("Expected to pull a message, instead got nothing")
			}else if messages[0].Message != "hello from test" {
				t.Error("Expected hello from test got ", messages[0].Message)
			}
		}()
	}
	// push a message to all 100 listeners
	// each one should receive the message
	// a fan-out process
	messageId := ps.Push("TestPull", "hello from test")
	assert.Equal(t, messageId, ps.LastMessageId("TestPull"))

	// check that there is no other messages on the que
	remainingMessages := ps.PullSince("TestPull", 1, messageId)
	assert.Equal(t, len(remainingMessages), 0)

	<-time.After(time.Millisecond * 5000)
}
/*
func TestTimeout(t *testing.T) {
	runtime.GOMAXPROCS(4)
	//t.SkipNow()
	done := make(chan int)
	go func() {
		ps := NewPubySuby()
		timedOut := ps.Pull("TestTimeout", 5)
		if timedOut != nil {
			t.Error("Expected nil due timeout on empty topic, got ", timedOut)
		}
		close(done)
	}()
	<-done
}

func TestPullExplicitClose(t *testing.T) {
	runtime.GOMAXPROCS(4)
	//t.SkipNow()
	done := make(chan int)
	go func() {
		ps := NewPubySuby()
		ps.Push("TestPullExplicitClose", "one")
		ps.Push("TestPullExplicitClose", "two")
		ps.Push("TestPullExplicitClose", "three")
		messages := ps.Pull("TestPullExplicitClose", 0)
		if len(messages) != 3 {
			t.Error("Expected 3 messages, got ", len(messages))
		}
		close(done)
	}()
	<-done
}

func TestPullSinceAndGC(t *testing.T) {
	//t.Parallel()
	//t.SkipNow()
	runtime.GOMAXPROCS(4)

	ps := NewPubySuby()
	lastMessageId := ps.Push("Test", "one")
	assert.Equal(t, lastMessageId, ps.LastMessageId("Test"))

	go func() {
		// wait for 100 milliseconds in goroutine
		results := ps.PullSince("Test", 100, lastMessageId)
		if len(results) != 0 {
			t.Error("Expected 0 messages, got ", len(results))
		}
		for i := 0; i < 100; i++ {
			ps.Push("Test", strconv.Itoa(i))
			<-time.After(time.Millisecond * 10)
		}
		results = ps.PullSince("Test", -100, lastMessageId)
	}()
	<-time.After(time.Second * 5)
}
*/

func unused_imports() {
	var _ = fmt.Printf
	var _ = time.Saturday
	var _ = strconv.Atoi
	var _ = assert.Equal
	var _ = log.Printf
}
