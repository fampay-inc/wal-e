package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	wal_e "git.famapp.in/fampay-inc/wal-e"
)

func WalStandyStatusUpdateCounter(_ context.Context, _ string, _ string) {

}

func ReplicaLagMetricFunc(_ context.Context, _ int64) {

}

func TestMsg(logs *wal_e.Log) {
	if logs.Wal != nil {
		fmt.Printf(">>>>>> %+v\n", logs.Wal.TableAction.Values)
	}
}

func RecoverFromPanic() func() {
	return func() {
		if rec := recover(); rec != nil {
		}
	}
}

func main() {
	ctx := context.Background()
	walConf := &wal_e.Config{
		Publications:          "tstore_pub_hb",
		ReplicationSlot:       "replication_slot_tstore_1",
		ReceiveMessageTimeout: time.Second * 1000,
	}
	masterDBUri := "postgresql://fampay:Fampay_Dev0101@localhost:5433/r41_1744019060_halfblood"
	walConsumer, err := wal_e.NewWalConsumer(ctx, masterDBUri, walConf)
	if err != nil {
		panic(err)
	}
	walConsumer.RecoverFromPanic = RecoverFromPanic
	err = walConsumer.InitConsumer()
	if err != nil {
		panic(err)
	}
	err = walConsumer.SendStandbyStatusUpdate()
	if err != nil {
		panic(err)
	}

	go walConsumer.ConsumerHealth.HealthHTTPServer(ctx, walConf)

	var wg sync.WaitGroup
	wg.Add(1)
	fmt.Println("> Starting wal consumption")
	go walConsumer.Consume(&wg)

	logChan := walConsumer.GetLogReceivceChannel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case log := <-logChan:
				if log != nil && log.Wal != nil {
					fmt.Printf("Received log: %+v\n", log.Wal.TableAction.Values)
					for k, v := range log.Wal.TableAction.Values {
						fmt.Println(k, v)
					}

					fmt.Println(">>>>>>>>>>>>>>>")
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()
}
