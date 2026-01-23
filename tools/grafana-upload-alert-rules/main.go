package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"

	"github.com/grafana/grafana-openapi-client-go/client/provisioning"
	"github.com/grafana/grafana-openapi-client-go/models"

	"github.com/malonaz/core/go/flags"
	"github.com/malonaz/core/go/grafana"
	"github.com/malonaz/core/go/jsonnet"
	"github.com/malonaz/core/go/logging"
)

var opts struct {
	Logging           *logging.Opts `group:"Logging" namespace:"logging" env-namespace:"LOGGING"`
	Grafana           *grafana.Opts `group:"Grafana" namespace:"grafana" env-namespace:"GRAFANA"`
	Folder            string        `long:"folder" description:"Folder to upload dashboard to"`
	RuleGroup         string        `long:"rule-group" description:"Rule group"`
	AlertRuleFilepath string        `long:"alert-rule-filepath" description:"path to the alert rule we wish to upload"`
	ConfigFilepath    string        `long:"config-filepath" description:"path to the alert config"`
}

type Config struct {
	ContactPoints          []*models.EmbeddedContactPoint `json:"contactPoints"`
	NotificationPolicyTree *models.Route                  `json:"notificationPolicyTree"`
	MessageTemplates       []*models.NotificationTemplate `json:"templateMessages"`
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
	if err := logging.Init(opts.Logging); err != nil {
		return err
	}

	client, err := grafana.NewClient(opts.Grafana)
	if err != nil {
		slog.ErrorContext(ctx, "creating client", "error", err)
		return err
	}

	if opts.AlertRuleFilepath != "" {
		if opts.Folder == "" {
			slog.ErrorContext(ctx, "missing --folder")
			os.Exit(1)
		}
		if opts.RuleGroup == "" {
			slog.ErrorContext(ctx, "missing --rule-group")
			os.Exit(1)
		}

		folder, err := client.CreateFolderIfNotExist(ctx, opts.Folder)
		if err != nil {
			slog.ErrorContext(ctx, "getting folders", "error", err)
			return err
		}

		dataSources, err := client.Datasources.GetDataSources()
		if err != nil {
			slog.ErrorContext(ctx, "getting data sources", "error", err)
			return err
		}
		dataSourceNameToDataSource := map[string]*models.DataSourceListItemDTO{}
		for _, dataSource := range dataSources.Payload {
			dataSourceNameToDataSource[dataSource.Name] = dataSource
		}

		alertRules, err := client.Provisioning.GetAlertRules()
		if err != nil {
			slog.ErrorContext(ctx, "getting alert rules", "error", err)
			return err
		}
		alertRuleTitleToAlertRule := map[string]*models.ProvisionedAlertRule{}
		for _, alertRule := range alertRules.Payload {
			alertRuleTitleToAlertRule[*alertRule.Title] = alertRule
		}

		bytes, err := os.ReadFile(opts.AlertRuleFilepath)
		if err != nil {
			slog.ErrorContext(ctx, "reading file", "error", err)
			return err
		}
		alertRule := &models.ProvisionedAlertRule{}
		if err := json.Unmarshal(bytes, alertRule); err != nil {
			slog.ErrorContext(ctx, "unmarshaling alert rule", "error", err)
			return err
		}
		alertRule.FolderUID = &folder.UID
		alertRule.RuleGroup = &opts.RuleGroup
		for _, alertQuery := range alertRule.Data {
			if dataSource, ok := dataSourceNameToDataSource[alertQuery.DatasourceUID]; ok {
				alertQuery.DatasourceUID = dataSource.UID
			}
		}

		if existing, ok := alertRuleTitleToAlertRule[*alertRule.Title]; ok {
			request := &provisioning.PutAlertRuleParams{
				Context: ctx,
				UID:     existing.UID,
				Body:    alertRule,
			}
			if _, err := client.Provisioning.PutAlertRule(request); err != nil {
				slog.ErrorContext(ctx, "updating alert rule", "title", *alertRule.Title, "error", err)
				return err
			}
			slog.InfoContext(ctx, "updated alert rule", "title", *alertRule.Title)
			return nil
		}

		request := &provisioning.PostAlertRuleParams{
			Context: ctx,
			Body:    alertRule,
		}
		if _, err := client.Provisioning.PostAlertRule(request); err != nil {
			slog.ErrorContext(ctx, "creating alert rule", "title", *alertRule.Title, "error", err)
			return err
		}
		slog.InfoContext(ctx, "created alert rule", "title", *alertRule.Title)
		return nil
	}

	bytes, err := os.ReadFile(opts.ConfigFilepath)
	if err != nil {
		slog.ErrorContext(ctx, "reading file", "error", err)
		return err
	}
	config := &Config{}
	bytes, err = jsonnet.EvaluateSnippet(string(bytes))
	if err != nil {
		slog.ErrorContext(ctx, "evaluating snippet", "error", err)
		return err
	}
	if err := json.Unmarshal(bytes, config); err != nil {
		slog.ErrorContext(ctx, "unmarshaling config", "error", err)
		return err
	}

	request := &provisioning.GetContactpointsParams{Context: ctx}
	contactPoints, err := client.Provisioning.GetContactpoints(request)
	if err != nil {
		slog.ErrorContext(ctx, "getting contact points", "error", err)
		return err
	}
	contactPointNameToContactPoint := map[string]*models.EmbeddedContactPoint{}
	for _, contactPoint := range contactPoints.Payload {
		contactPointNameToContactPoint[contactPoint.Name] = contactPoint
	}

	for _, contactPoint := range config.ContactPoints {
		if existing, ok := contactPointNameToContactPoint[contactPoint.Name]; ok {
			request := &provisioning.PutContactpointParams{
				Context: ctx,
				UID:     existing.UID,
				Body:    contactPoint,
			}
			if _, err := client.Provisioning.PutContactpoint(request); err != nil {
				slog.ErrorContext(ctx, "updating contact point", "name", contactPoint.Name, "error", err)
				return err
			}
			slog.InfoContext(ctx, "updated contact point", "name", contactPoint.Name)
			continue
		}

		request := &provisioning.PostContactpointsParams{
			Context: ctx,
			Body:    contactPoint,
		}
		if _, err := client.Provisioning.PostContactpoints(request); err != nil {
			slog.ErrorContext(ctx, "creating contact point", "name", contactPoint.Name, "error", err)
			return err
		}
		slog.InfoContext(ctx, "created contact point", "name", contactPoint.Name)
	}

	putPolicyTreeRequest := &provisioning.PutPolicyTreeParams{
		Context: ctx,
		Body:    config.NotificationPolicyTree,
	}
	if _, err := client.Provisioning.PutPolicyTree(putPolicyTreeRequest); err != nil {
		slog.ErrorContext(ctx, "setting the notification policy tree", "error", err)
		return err
	}
	slog.InfoContext(ctx, "updated notification policy tree")

	for _, messageTemplate := range config.MessageTemplates {
		request := &provisioning.PutTemplateParams{
			Context: ctx,
			Name:    messageTemplate.Name,
			Body: &models.NotificationTemplateContent{
				Template: messageTemplate.Template,
			},
		}
		if _, err := client.Provisioning.PutTemplate(request); err != nil {
			slog.ErrorContext(ctx, "putting template", "name", messageTemplate.Name, "error", err)
			return err
		}
		slog.InfoContext(ctx, "updated message template", "name", messageTemplate.Name)
	}

	return nil
}
