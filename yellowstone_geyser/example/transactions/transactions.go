package main

import (
	"context"
	"fmt"
	"time"

	"github.com/B-RCompany/goyser/yellowstone_geyser"
	geyser_pb "github.com/B-RCompany/goyser/yellowstone_geyser/pb"
	"google.golang.org/grpc/metadata"
)

func main() {
	TransactionsSub()
}

func TransactionsSub() {
	ctx := context.Background()

	// url := "https://solana-yellowstone-grpc.publicnode.com:443"
	// _metadata := metadata.New(nil)

	url := "https://grpc.fra.shyft.to"
	xToken := ""
	_metadata := metadata.New(nil)

	client, err := yellowstone_geyser.New(ctx, url, xToken, _metadata)
	if err != nil {
		fmt.Printf("err %+v", err)

		return
	}

	if err = client.AddStreamClient(ctx, "test", geyser_pb.CommitmentLevel_PROCESSED); err != nil {
		fmt.Printf("err %+v", err)

		return
	}

	streamClient := client.GetStreamClient("test")
	if streamClient == nil {
		fmt.Printf("Geyser client does not have a stream named %s", "test")

		return
	}

	failed := false 

	accounts := []string{"5QfWopLLtM5E6i6ZXeWwAr5cJ3YvKMaRnw6EFuPYef3d"}

	err = streamClient.SubscribeTransaction(
		"balances",
		&geyser_pb.SubscribeRequestFilterTransactions{
			Failed:         &failed,
			AccountInclude: accounts,
		},
	)
	if err != nil {
		fmt.Printf("err %+v", err)

		return
	}

	go func() {
		for {
			select {
			case recv := <-streamClient.Ch:
				fmt.Printf("streamClient.Ch %+v\n", recv)
			}
		}
	}()

	time.Sleep(120 * time.Second)
}
