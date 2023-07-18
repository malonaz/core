package main

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/grafana-tools/sdk"

	"common/go/flags"
	"common/go/logging"
)

var log = logging.NewLogger()

var opts struct {
	GrafanaAPIKey     string `long:"grafana-api-key" description:"Grafana API key" required:"true"`
	GrafanaAPIURL     string `long:"grafana-api-url" description:"Grafana API url" required:"true"`
	GrafanaFolder     string `long:"grafana-folder" description:"Folder to upload dashboard to"`
	DashboardFilepath string `long:"dashboard-filepath" description:"path to the dashboard we wish to upload" required:"true"`
	TimeoutSeconds    int64  `long:"timeout-seconds" description:"import timeout" default:"10"`
}

func main() {
	flags.MustParse(&opts)
	client, err := sdk.NewClient(opts.GrafanaAPIURL, opts.GrafanaAPIKey, sdk.DefaultHTTPClient)
	if err != nil {
		log.Panicf("instantiating grafana client: %v")
	}
	bytes, err := os.ReadFile(opts.DashboardFilepath)
	if err != nil {
		log.Panicf("reading file: %v", err)
	}
	board := &sdk.Board{}
	if err := json.Unmarshal(bytes, board); err != nil {
		log.Panicf("unmarshaling board: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(opts.TimeoutSeconds)*time.Second)
	defer cancel()

	// Create folder if it doesn't exist.
	folderID := sdk.DefaultFolderId
	folderName := "General"
	if opts.GrafanaFolder != "" {
		folderName = opts.GrafanaFolder
		folders, err := client.GetAllFolders(ctx)
		if err != nil {
			log.Panicf("getting folders: %v", err)
		}
		for _, folder := range folders {
			if folder.Title == opts.GrafanaFolder {
				folderID = folder.ID
				break
			}
		}
		if folderID == sdk.DefaultFolderId {
			// We must create the folder.
			folder := sdk.Folder{Title: opts.GrafanaFolder}
			var err error
			folder, err = client.CreateFolder(ctx, folder)
			if err != nil {
				log.Panicf("creating folder: %v", err)
			}
			folderID = folder.ID
			if folderID == sdk.DefaultFolderId {
				log.Panic("folder created did not return an id")
			}
			log.Infof("created folder: %s", opts.GrafanaFolder)
		}
	}

	params := sdk.SetDashboardParams{
		FolderID:  folderID,
		Overwrite: true,
	}
	response, err := client.SetDashboard(ctx, *board, params)
	if err != nil {
		log.Panicf("uploading dashboard to grafana: %v", err)
	}
	log.Infof("uploaded dashboard [%s/%s] @ %s%s", folderName, board.Title, opts.GrafanaAPIURL, *response.URL)
}
