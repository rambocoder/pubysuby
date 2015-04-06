package prototypes

import (
	"github.com/stretchr/testify/assert"
	"log"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

func TestRand(t *testing.T) {
	runtime.GOMAXPROCS(16)
	// http://blog.golang.org/go-concurrency-patterns-timing-out-and
	// one channel to receive first results from
	ch := make(chan int, 1)
	closed := make(chan int)
	go func() {
		//close(closed)
		closed <- 10
		log.Println("Ended")
	}()
	for i := 1; i < 10; i++ {
		go func() {
			select {
			// the 1 is needed because
			// doWork() result can come back before <- ch happens
			// therefor a need for 1 buffer size
			case ch <- doWork(): // non blocking send into ch
				log.Println("Received")
			default:
				log.Println("Default")
			}
		}()
	}
	//v, ok := <-closed // ok will be false if the chan is closed, otherwise it is true
	//log.Println("Value:", v, " ok", ok)
	value := <-ch
	assert.Equal(t, 1, value, "Expected 1 got: ", value)
}

func doWork() int {
	wait := rand.Intn(1000)
	var _ = wait
	<-time.After(time.Millisecond * time.Duration(1000))
	return 1
}
