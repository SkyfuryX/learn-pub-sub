package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()
	channel, err := conn.Channel()
	if err != nil {
		fmt.Println(err)
		return
	}


	fmt.Println("Connected!")
	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println(err)
		return
	}

	gameState := gamelogic.NewGameState(userName)
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, "pause."+userName, routing.PauseKey, pubsub.TransientQueue, handlerPause(gameState))
	if err != nil {
		fmt.Println(err)
		return
	}
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, "army_moves."+userName, "army_moves.*", pubsub.TransientQueue, handlerMove(gameState, channel))
	if err != nil {
		fmt.Println(err)
		return
	}

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic,"war", routing.WarRecognitionsPrefix+".*", pubsub.DurableQueue, handlerWar(gameState, channel))
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		cmds := gamelogic.GetInput()
		if len(cmds) == 0 {
			continue
		}
		switch cmds[0] {
		case "spawn":
			err = gameState.CommandSpawn(cmds)
			if err != nil {
				fmt.Println(err)
			}
		case "move":
			move, err := gameState.CommandMove(cmds)
			if err != nil {
				fmt.Println(err)
			} else {
				pubsub.PublishJSON(channel, routing.ExchangePerilTopic, "army_moves."+userName, move)
				fmt.Printf("Move Published: %v\n", strings.Join(cmds, " "))
			}
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not alowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			fmt.Println("Shutting down client...")
			os.Exit(0)
		default:
			fmt.Println("Command not understood. Please try again.")
		}
	}

	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	_ = <-signalChan
	fmt.Println("Shutting down client...")
}
