// curl "http://localhost:8080/Pull/?topic=test&timeout=5"
// curl "http://localhost:8080/PullSince/?since=5&topic=test&timeout=5"
// curl "http://localhost:8080/Sub"
// curl -d "topic=test&message=Hello" http://localhost:8080/Push
// ab -c 500 -n 10000 "http://localhost:8080/Pull/?topic=test&timeout=0"

package main

import (
	"fmt"
	"github.com/rambocoder/pubysuby"
	"github.com/gorilla/mux"
	"html"
	"log"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"os"
	"io"
	"encoding/json"
	"io/ioutil"
)

var ps *pubysuby.PubySuby

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/", HomeHandler).Methods("GET")
	http.Handle("/", r)

	fs := http.FileServer(http.Dir("static"))
	http.Handle("/static/", http.StripPrefix("/static/", fs))

	runtime.GOMAXPROCS(64)

	ps = pubysuby.NewPubySuby()

	http.HandleFunc("/pull/", HandlePull)
	http.HandleFunc("/chat/pullsince", HandlePullSince)
	http.HandleFunc("/Push", HandlePush)
	http.HandleFunc("/Sub", HandleSub)
	http.HandleFunc("/chat/start", HandleLastMessageId)
	http.HandleFunc("/chat/push", HandlePushJson)
	fmt.Println("Listening on http://localhost:8888")
	http.ListenAndServe("localhost:8888", nil)
}

func HomeHandler(w http.ResponseWriter, r *http.Request) {
	file, err := os.Open("static/index.html")
	defer file.Close()
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	io.Copy(w, file)
}

func HandlePush(w http.ResponseWriter, r *http.Request) {
	topicName := r.FormValue("topic")
	content := r.FormValue("message")
	fmt.Fprintf(w, "That Message Id is: %d on topic: %q \n", ps.Push(topicName, content), topicName)
}

func getQuery(r *http.Request, name string) string {
	query_params, _ := url.ParseQuery(r.URL.RawQuery)
	arg_topic, ok_topic := query_params[name]
	if !ok_topic {
		fmt.Println("No ", name, " sent: ", r.URL.Path)
		return ""
	}
	return arg_topic[0]
}

func HandleSub(w http.ResponseWriter, r *http.Request) {
	myListenChannel := ps.Sub("test")
	defer ps.Unsubscribe("test", myListenChannel)
	// Ideal API for pubysuby
	// subscription := ps.Sub("test")
	// defer subscription.Unsubscribe()
	// <- subscription.inChan
	i := 0
	for {
		i++
		updates, ok := <-myListenChannel
		if !ok {
			log.Println("Updates melted down")
			ps.Unsubscribe("test", myListenChannel)
			break
		}

		fmt.Println("Send to browser " + updates[0].Message)

		fmt.Fprintf(w, updates[0].Message)

		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}

		fmt.Println("Updates in sub:", updates)
		if i > 5 {
			log.Println("After 5 updates, just gonna turn off the Sub")
			break
		}
		fmt.Fprintf(w, "Message Id: %d is: %q on topic: %q \n", updates[0].MessageId, updates[0].Message, html.EscapeString("test"))
	}
}

func HandlePull(w http.ResponseWriter, r *http.Request) {

	topicName := getQuery(r, "topic")
	timeout := getQuery(r, "timeout")

	wait, _ := strconv.ParseInt(timeout, 10, 64)


	var messages []pubysuby.TopicItem
	messages = ps.Pull(topicName, wait)

	for _, v := range messages {
		fmt.Fprintf(w, "Message Id: %d is: %q on topic: %q \n", v.MessageId, v.Message, html.EscapeString(topicName))
	}

}



func HandleLastMessageId(w http.ResponseWriter, r *http.Request) {

	// topic name here is hardcoded to test
	fmt.Printf("Last Message Id is: %d on topic: %s \n", ps.LastMessageId("test"), html.EscapeString("test"))

	fmt.Fprintf(w, `{"LastMessageId":"%s"}`,  strconv.FormatInt(ps.LastMessageId("test"), 10) )

}



func HandlePushJson(w http.ResponseWriter, r *http.Request) {
	//topicName := r.FormValue("topic")
	//content := r.FormValue("message")

	m := Message{}

	b, _ := ioutil.ReadAll(r.Body)
	jdata := string(b)
	log.Println("Received: " + jdata)

	err := json.Unmarshal([]byte(jdata), &m)
	if err != nil {
		log.Fatal(err)
	}


	//json.NewDecoder(r.Body).Decode(&m)

	fmt.Println("Message is: " + m.Message)

	fmt.Fprintf(w, `{"ok":"true"}`)

	fmt.Printf( "That Message Id is: %d on topic: %q \n", ps.Push("test", m.Message), "test")
}

type Message struct {
	Message string `json:"message"`
}

func HandlePullSince(w http.ResponseWriter, r *http.Request) {

	topicName := getQuery(r, "topic")
	timeout := getQuery(r, "timeout")
	since := getQuery(r, "since")
	wait, _ := strconv.ParseInt(timeout, 10, 64)
	lastMessageId, _ := strconv.ParseInt(since, 10, 64)
	var messages []pubysuby.TopicItem
	messages = ps.PullSince(topicName, wait, lastMessageId)

	if len(messages) > 0 {

		for _, v := range messages {
			fmt.Println("Mesage to send out" + v.Message)
			fmt.Printf("Message Id: %d is: %q on topic: %q \n", v.MessageId, v.Message, html.EscapeString(topicName))

			fmt.Fprintf(w, `{"ok":{"text":"%s"}, "timestamp":"%d"}`, v.Message, v.MessageId)
		}
	}else {
		fmt.Printf(`{"ok":"timeout", "timestamp":"%d"}\n`, lastMessageId)
		fmt.Fprintf(w, `{"ok":"timeout", "timestamp":"%d"}`, lastMessageId)
	}

}
