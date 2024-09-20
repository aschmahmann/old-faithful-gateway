package main

import (
	"github.com/ipfs/go-cid"
	"github.com/urfave/cli/v2"
	"log"
	"os"
)

const name = "old-faithful-gateway"
const version = "0.1"

func main() {
	app := &cli.App{
		Name:    name,
		Usage:   "A Delegated Routing V1 server and proxy for all your routing needs.",
		Version: version,
		Commands: []*cli.Command{
			{
				Name: "start",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "gateway-address",
						Value:   "127.0.0.1:8190",
						EnvVars: []string{"OF_GATEWAY_ADDRESS"},
						Usage:   "gateway address",
					},
					&cli.IntFlag{
						Name:  "epoch",
						Value: 429,
						Usage: "solana epoch to serve",
					},
					&cli.StringFlag{
						Name:  "epoch-cid",
						Value: "bafyreiafhaegz5uxvmfjnx7wnvmwizjo7mbsac3mjlvmr4443aisyfvsxq",
						Usage: "solana epoch to serve",
					},
				},
				Action: func(ctx *cli.Context) error {
					listenAddr := ctx.String("gateway-address")
					epoch := ctx.Int("epoch")
					epochCidStr := ctx.String("epoch-cid")
					epochCid, err := cid.Decode(epochCidStr)
					if err != nil {
						return err
					}

					return startGateway(ctx.Context, listenAddr, epoch, epochCid)
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
