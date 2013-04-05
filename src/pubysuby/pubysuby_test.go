package pubysuby

import (
	"testing"
	"runtime"
	"time"
	"strconv"

	"fmt"
)

func TestSubDeath(t *testing.T) {
	go func() {
		ps := New()
		ch := ps.Sub("TestSubDeath")
		go func(p *PubySuby) {
			for {
				p.Push("TestSubDeath", "Hello")
				<-time.After(time.Second*1)
			}
		}(ps)
		// receive once
		<-ch
	}()
	<-time.After(time.Second*10)
	fmt.Println("Finished TestSubDeath")
}

func TestSub(t *testing.T) {
	runtime.GOMAXPROCS(4)
	for y := 0; y < 100; y++ {
		go func(y1 int) {
			ps := New()
			ch := ps.Sub("TestSub" + strconv.FormatInt(int64(y1), 10))
			go func(y2 int) {
				for i := 0; i < 10; i++ {
					ps.Push("TestSub" + strconv.FormatInt(int64(y2), 10), "ok")
				}
			}(y1)
			for i := 0; i < 10; i++ {
				select {
				case <-ch:
				case <-time.After(time.Second*1):
					t.Error("Timedout in  TestSub", i, "of", y1)
				}
			}
		}(y)
	}
	<-time.After(time.Second*10)
	fmt.Println("Finished TestSub")
}

func TestPullZeroTimeout(t *testing.T) {
	runtime.GOMAXPROCS(4)
	go func() {
		ps := New()
		for i := 0; i < 100; i++ {
			go func(i1 int) {

				go func() {
					messages := ps.Pull("TestPullZeroTimeout" + strconv.FormatInt(int64(i1), 10), 0)
					if (messages == nil) {
						// empty channel
					} else if (messages[0].Message != "TestPullZeroTimeout") {
						t.Error("Expected TestPullZeroTimeout got ", messages[0].Message)
					}
				}()
				<-time.After(time.Second*1)
				result := ps.Push("TestPullZeroTimeout" + strconv.FormatInt(int64(i1), 10), "TestPullZeroTimeout")
				if result < 1 {
					t.Error("Expected greater than 0 from publish, got", result)
				}

			}(i)

		}

	}()
	<-time.After(time.Second*10)
	fmt.Println("Finished TestPullZeroTimeout")
}

func TestPull(t *testing.T) {
	runtime.GOMAXPROCS(4)
	go func() {
		ps := New()
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
	<-time.After(time.Second*10)
}

func TestTimeout(t *testing.T) {
	runtime.GOMAXPROCS(4)
	go func() {
		ps := New()
		timedOut := ps.Pull("TestTimeout", 5)
		if (timedOut != nil) {
			t.Error("Expected timedout from test got ", timedOut)
		}
	}()
	<-time.After(time.Second*10)
}



func TestPullExplicitClose(t *testing.T) {
	runtime.GOMAXPROCS(4)



	go func() {
		ps := New()
		ps.Push("TestPullExplicitClose", "one")
		ps.Push("TestPullExplicitClose", "two")
		ps.Push("TestPullExplicitClose", "three")
		messages := ps.Pull("TestPullExplicitClose", 0)
		if (len(messages) != 3) {
			t.Error("Expected 3 messages, got ", len(messages))
		}

	}()
	<-time.After(time.Second*10)
}


