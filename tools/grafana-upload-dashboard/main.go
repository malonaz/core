package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"

	"github.com/grafana/grafana-openapi-client-go/models"

	"github.com/malonaz/core/go/flags"
	"github.com/malonaz/core/go/grafana"
)

var opts struct {
	Grafana           *grafana.Opts `group:"Grafana" namespace:"grafana" env-namespace:"GRAFANA"`
	Folder            string        `long:"folder" description:"Folder to upload dashboard to"`
	DashboardFilepath string        `long:"dashboard-filepath" description:"path to the dashboard we wish to upload" required:"true"`
}

func main() {
	ctx := context.Background()
	if err := run(ctx); err != nil {
		slog.ErrorContext(ctx, "running", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	if err := flags.Parse(&opts); err != nil {
		return err
	}
	client, err := grafana.NewClient(opts.Grafana)
	if err != nil {
		return err
	}

	bytes, err := os.ReadFile(opts.DashboardFilepath)
	if err != nil {
		return err
	}
	importDashboardRequest := &models.ImportDashboardRequest{
		Dashboard: map[string]any{},
	}
	if err := json.Unmarshal(bytes, &importDashboardRequest.Dashboard); err != nil {
		return err
	}

	folder, err := client.CreateFolderIfNotExist(ctx, opts.Folder)
	if err != nil {
		return err
	}

	importDashboardRequest.Overwrite = true
	importDashboardRequest.FolderUID = folder.UID
	response, err := client.Dashboards.ImportDashboard(importDashboardRequest)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "uploaded dashboard", "folder", folder.Title, "title", response.Payload.Title, "url", client.BaseURL()+response.Payload.ImportedURL)
	return nil
}
