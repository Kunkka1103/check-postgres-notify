package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/lib/pq"
)

var dsn = flag.String("dsn", "", "PostgreSQL DSN")
var dingdingURL = flag.String("dingdingURL", "", "DingTalk webhook URL")

func main() {
	// 解析命令行参数
	flag.Parse()

	// 创建 PostgreSQL 监听器
	listener := pq.NewListener(*dsn, 10*time.Second, time.Minute, reportProblem)
	err := listener.Listen("sql_alert")
	if err != nil {
		log.Fatalf("Failed to listen to channel: %v", err)
	}

	fmt.Println("Waiting for notifications on channel 'sql_alert'...")

	for {
		waitForNotification(listener, *dingdingURL)
	}
}

func reportProblem(ev pq.ListenerEventType, err error) {
	if err != nil {
		log.Fatalf("Listener error: %v", err)
	}
}

func waitForNotification(listener *pq.Listener, dingdingURL string) {
	for {
		select {
		case n := <-listener.Notify:
			if n != nil {
				fmt.Printf("Got NOTIFY: %s\n", n.Extra)

				// 发送钉钉告警
				err := sendDingdingAlert(n.Extra, dingdingURL)
				if err != nil {
					log.Printf("Failed to send alert to DingTalk: %v", err)
				}
			}
		case <-time.After(90 * time.Second):
			go func() {
				err := listener.Ping()
				if err != nil {
					log.Printf("Failed to ping listener: %v", err)
				}
			}()
			fmt.Println("No new notifications, listener ping sent.")
		}
	}
}

func sendDingdingAlert(message, dingdingURL string) error {
	payload := map[string]interface{}{
		"msgtype": "text",
		"text": map[string]string{
			"content": fmt.Sprintf("Alert: %s", message),
		},
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := http.Post(dingdingURL, "application/json", bytes.NewBuffer(payloadBytes))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received non-200 response code: %d", resp.StatusCode)
	}

	fmt.Println("Alert sent to DingTalk successfully.")
	return nil
}
