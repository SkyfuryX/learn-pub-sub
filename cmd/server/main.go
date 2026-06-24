package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()
	fmt.Print("Connection started\n")
	channel, err := conn.Channel()
	if err != nil {
		fmt.Println(err)
		return
	}
	_, _, err = pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, "game_logs", "game_logs.*", pubsub.DurableQueue)
	if err != nil {
		fmt.Println(err)
	}

	gamelogic.PrintServerHelp()
	for {
		cmds := gamelogic.GetInput()
		if len(cmds) == 0 {
			continue
		}
		switch cmds[0] {
		case "pause":
			fmt.Println("Pausing game")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				fmt.Println(err)
				return

			}
		case "resume":
			fmt.Println("Resuming game")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				fmt.Println(err)
				return

			}
		case "quit":
			fmt.Println("Shutting down server...")
			os.Exit(0)
		default:
			fmt.Println("Command not understood. Try again.")
		}
	}

	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	_ = <-signalChan
	fmt.Println("Shutting down server...")
}
