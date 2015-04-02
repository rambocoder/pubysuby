package pubysuby

import (
	"runtime"
	"strconv"
	"testing"
	"time"

	"fmt"
	"github.com/stretchr/testify/assert"
)

func TestSubDeath(t *testing.T) {
	go func() {

		ps := NewPubySuby()
		ch := ps.Sub("TestSubDeath")
		go func(p *PubySuby) {
			for {
				p.Push("TestSubDeath", "Hello")
				<-time.After(time.Second * 1)
			}
		}(ps)
		// receive once
		<-ch
	}()
	<-time.After(time.Second * 1)
	fmt.Println("Finished TestSubDeath")
}

func TestUnsubscribe2(t *testing.T) {

	go func() {

		ps := NewPubySuby()
		ch := ps.Sub("TestUnsubscribe")
		go func(p *PubySuby) {
			for {
				<-time.After(time.Second * 1)
				p.Push("TestSubDeath", "Hello")

			}
			p.Unsubscribe("TestUnsubscribe", nil)
		}(ps)
		// receive once
		<-ch
	}()
	<-time.After(time.Second * 1)
	fmt.Println("Finished TestUnsubscribe")
}

func TestSub(t *testing.T) {
	runtime.GOMAXPROCS(4)
	for y := 0; y < 100; y++ {
		go func(y1 int) {
			ps := NewPubySuby()
			ch := ps.Sub("TestSub" + strconv.FormatInt(int64(y1), 10))
			go func(y2 int) {
				for i := 0; i < 10; i++ {
					ps.Push("TestSub"+strconv.FormatInt(int64(y2), 10), "ok")
				}
			}(y1)
			for i := 0; i < 10; i++ {
				select {
				case <-ch:
				case <-time.After(time.Second * 1):
					t.Error("Timedout in  TestSub", i, "of", y1)
				}
			}
		}(y)
	}
	<-time.After(time.Second * 1)
	fmt.Println("Finished TestSub")
}

func TestPullZeroTimeout(t *testing.T) {
	runtime.GOMAXPROCS(4)
	go func() {
		ps := NewPubySuby()
		for i := 0; i < 100; i++ {
			go func(i1 int) {

				go func() {
					messages := ps.Pull("TestPullZeroTimeout"+strconv.FormatInt(int64(i1), 10), 0)
					if messages == nil {
						// empty channel
					} else if messages[0].Message != "TestPullZeroTimeout" {
						t.Error("Expected TestPullZeroTimeout got ", messages[0].Message)
					}
				}()
				<-time.After(time.Second * 1)
				result := ps.Push("TestPullZeroTimeout"+strconv.FormatInt(int64(i1), 10), "TestPullZeroTimeout")
				if result < 1 {
					t.Error("Expected greater than 0 from publish, got", result)
				}

			}(i)

		}

	}()
	<-time.After(time.Second * 1)
	fmt.Println("Finished TestPullZeroTimeout")
}

func TestPull(t *testing.T) {
	runtime.GOMAXPROCS(4)
	go func() {
		ps := NewPubySuby()
		for i := 0; i < 100; i++ {
			go func() {
				messages := ps.Pull("TestPull", 10)
				if messages[0].Message != "hello from test" {
					t.Error("Expected hello from test got ", messages[0].Message)
				}
			}()
		}
		ps.Push("TestPull", "hello from test")
	}()
	<-time.After(time.Second * 1)
}

func TestTimeout(t *testing.T) {
	runtime.GOMAXPROCS(4)
	go func() {
		ps := NewPubySuby()
		timedOut := ps.Pull("TestTimeout", 5)
		if timedOut != nil {
			t.Error("Expected timedout from test got ", timedOut)
		}
	}()
	<-time.After(time.Second * 1)
}

func TestPullExplicitClose(t *testing.T) {
	runtime.GOMAXPROCS(4)

	go func() {
		ps := NewPubySuby()
		ps.Push("TestPullExplicitClose", "one")
		ps.Push("TestPullExplicitClose", "two")
		ps.Push("TestPullExplicitClose", "three")
		messages := ps.Pull("TestPullExplicitClose", 0)
		if len(messages) != 3 {
			t.Error("Expected 3 messages, got ", len(messages))
		}

	}()
	<-time.After(time.Second * 1)
}

func TestPullSince(t *testing.T) {
	runtime.GOMAXPROCS(4)

	ps := NewPubySuby()
	lastMessageId := ps.Push("Test", "one")
	assert.Equal(t, lastMessageId, ps.LastMessageId("Test"))

	results := ps.PullSince("Test", 1, lastMessageId)
	if len(results) != 0 {
		t.Error("Expected 0 messages, got ", len(results))
	}
	ps.Push("Test", "two")
	results = ps.PullSince("Test", 1, lastMessageId)
	if len(results) != 1 {
		t.Error("Expected 0 messages, got ", len(results))
	}
	ps.Push("Test", "three")
	messages := ps.Pull("Test", 0)
	if len(messages) != 3 {
		t.Error("Expected 3 messages, got ", len(messages))
	}

}
