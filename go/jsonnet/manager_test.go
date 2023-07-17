package jsonnet

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/google/uuid"
	"github.com/nsf/jsondiff"
	"github.com/stretchr/testify/require"
)

var (
	tmpdir          = "/tmp/"
	jsondiffOptions = jsondiff.DefaultJSONOptions()

	simpleConfig   = []byte(`{"a": "b"}`)
	simpleConfig2  = []byte(`{"a": "c"}`)
	advancedConfig = []byte(`{"a": "b", "c": self.a}`)

	importedConfig  = []byte(`{"firstValue": 17.0, "secondValue": 11.0}`)
	importedConfig2 = []byte(`{"firstValue": 18.0, "secondValue": 12.0}`)
	importingConfig = []byte(`{
            local globals = import "./global_config.json",
            "firstValueMultiplied": globals.firstValue * 2,
            "secondValueMultiplied": globals.secondValue * 3
        }`)
	importingConfig2 = []byte(`{
            local globals = import "./global_config2.json",
            "firstValueMultiplied": globals.firstValue * 2,
            "secondValueMultiplied": globals.secondValue * 3
        }`)
)

type dummyConfig struct{ content []byte }

// ValidateAll implements the config interface.
func (c *dummyConfig) ValidateAll() error { return nil }

func passthroughParseFn(content []byte) (config, error) { return &dummyConfig{content: content}, nil }

func requireJSONEqual(t *testing.T, a, b any) {
	result, diff := jsondiff.Compare(a.(*dummyConfig).content, b.(*dummyConfig).content, &jsondiffOptions)
	require.Equal(t, jsondiff.FullMatch, result, "diff found: %s", diff)
}

func writeFile(t *testing.T, content []byte) (string, func()) {
	err := os.WriteFile(path.Join(tmpdir, "version"), []byte("1.2"), 0644)
	require.NoError(t, err)
	filepath := path.Join(tmpdir, uuid.New().String())
	err = os.WriteFile(filepath, content, 0644)
	require.NoError(t, err)
	return filepath, func() { os.Remove(filepath) }
}

func writeFileAtLocation(t *testing.T, filepath string, content []byte) func() {
	err := os.WriteFile(filepath, content, 0644)
	require.NoError(t, err)
	return func() { os.Remove(filepath) }
}

func TestLoading(t *testing.T) {
	t.Run("simple config", func(t *testing.T) {
		filepath, remove := writeFile(t, simpleConfig)
		defer remove()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		reloadableConfig := MustNewReloadableConfig(filepath, nil, passthroughParseFn)
		err := reloadableConfig.Start(ctx)
		require.NoError(t, err)
		config := reloadableConfig.GetConfig()
		requireJSONEqual(t, &dummyConfig{content: simpleConfig}, config.Payload)
		cancel()
	})
	t.Run("advanced config", func(t *testing.T) {
		filepath, remove := writeFile(t, advancedConfig)
		defer remove()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		reloadableConfig := MustNewReloadableConfig(filepath, nil, passthroughParseFn)
		err := reloadableConfig.Start(ctx)
		require.NoError(t, err)

		config := reloadableConfig.GetConfig()
		requireJSONEqual(t, &dummyConfig{content: []byte(`{"a": "b", "c": "b"}`)}, config.Payload)
	})

	t.Run("with data", func(t *testing.T) {
		filepath, remove := writeFile(t, importingConfig)
		defer remove()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		data := map[string]string{"./global_config.json": `{"firstValue": 17.0, "secondValue": 11.0}`}
		reloadableConfig := MustNewReloadableConfig(filepath, data, passthroughParseFn)
		err := reloadableConfig.Start(ctx)
		require.NoError(t, err)
		config := reloadableConfig.GetConfig()
		expected := []byte(`{
                    "firstValueMultiplied": 34,
                    "secondValueMultiplied": 33
                }`)
		requireJSONEqual(t, &dummyConfig{content: expected}, config.Payload)
	})
}

func TestHotReload(t *testing.T) {
	t.Run("entrypoint file change", func(t *testing.T) {
		filepath, remove := writeFile(t, simpleConfig)
		defer remove()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		reloadableConfig := MustNewReloadableConfig(filepath, nil, passthroughParseFn)
		err := reloadableConfig.Start(ctx)
		require.NoError(t, err)
		config := reloadableConfig.GetConfig()
		requireJSONEqual(t, &dummyConfig{content: simpleConfig}, config.Payload)

		// Change file.
		writeFileAtLocation(t, filepath, simpleConfig2)
		<-reloadableConfig.updateSignal // Wait for update.
		config = reloadableConfig.GetConfig()
		requireJSONEqual(t, &dummyConfig{content: simpleConfig2}, config.Payload)
	})

	t.Run("imported file change", func(t *testing.T) {
		remove := writeFileAtLocation(t, path.Join(tmpdir, "global_config.json"), importedConfig)
		defer remove()
		filepath, remove := writeFile(t, importingConfig)
		defer remove()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		reloadableConfig := MustNewReloadableConfig(filepath, nil, passthroughParseFn)
		err := reloadableConfig.Start(ctx)
		require.NoError(t, err)
		config := reloadableConfig.GetConfig()
		expected := []byte(`{
                    "firstValueMultiplied": 34,
                    "secondValueMultiplied": 33
                }`)
		requireJSONEqual(t, &dummyConfig{content: expected}, config.Payload)

		writeFileAtLocation(t, path.Join(tmpdir, "global_config.json"), importedConfig2)
		<-reloadableConfig.updateSignal // Wait for update.
		config = reloadableConfig.GetConfig()
		expected = []byte(`{
                    "firstValueMultiplied": 36,
                    "secondValueMultiplied": 36
                }`)
		requireJSONEqual(t, &dummyConfig{content: expected}, config.Payload)
	})

	t.Run("change imports new file -> ensure we watch that file", func(t *testing.T) {
		remove := writeFileAtLocation(t, path.Join(tmpdir, "global_config.json"), importedConfig)
		defer remove()
		filepath, remove := writeFile(t, importingConfig)
		defer remove()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		reloadableConfig := MustNewReloadableConfig(filepath, nil, passthroughParseFn)
		err := reloadableConfig.Start(ctx)
		require.NoError(t, err)
		config := reloadableConfig.GetConfig()
		expected := []byte(`{
                    "firstValueMultiplied": 34,
                    "secondValueMultiplied": 33
                }`)
		requireJSONEqual(t, &dummyConfig{content: expected}, config.Payload)

		// Write a new imported file. Note the different name below.
		writeFileAtLocation(t, path.Join(tmpdir, "global_config2.json"), importedConfig2)
		// Modify the entrypoint file, which now imports the `global_config2.json` file.
		writeFileAtLocation(t, filepath, importingConfig2)
		<-reloadableConfig.updateSignal // Wait for update.
		config = reloadableConfig.GetConfig()
		expected = []byte(`{
                    "firstValueMultiplied": 36,
                    "secondValueMultiplied": 36
                }`)
		requireJSONEqual(t, &dummyConfig{content: expected}, config.Payload)

		// Modify the imported file to ensure it has been picked up by the file watchdog.
		writeFileAtLocation(t, path.Join(tmpdir, "global_config2.json"), importedConfig)
		<-reloadableConfig.updateSignal // Wait for update.
		config = reloadableConfig.GetConfig()
		expected = []byte(`{
                    "firstValueMultiplied": 34,
                    "secondValueMultiplied": 33
                }`)
		requireJSONEqual(t, &dummyConfig{content: expected}, config.Payload)
	})
}

func TestUpdateData(t *testing.T) {
	t.Run("reloads on data change", func(t *testing.T) {
		filepath, remove := writeFile(t, importingConfig)
		defer remove()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		data := map[string]string{"./global_config.json": `{"firstValue": 17.0, "secondValue": 11.0}`}
		reloadableConfig := MustNewReloadableConfig(filepath, data, passthroughParseFn)
		err := reloadableConfig.Start(ctx)
		require.NoError(t, err)
		config := reloadableConfig.GetConfig()
		expected := []byte(`{
                    "firstValueMultiplied": 34,
                    "secondValueMultiplied": 33
                }`)
		requireJSONEqual(t, &dummyConfig{content: expected}, config.Payload)

		reloadableConfig.UpdateData("./global_config.json", `{"firstValue": 18.0, "secondValue": 12.0}`)
		<-reloadableConfig.updateSignal // Wait for update.
		config = reloadableConfig.GetConfig()
		expected = []byte(`{
                    "firstValueMultiplied": 36,
                    "secondValueMultiplied": 36
                }`)
		requireJSONEqual(t, &dummyConfig{content: expected}, config.Payload)
	})
}

func TestShouldMeasureEvaluationTime(t *testing.T) {
	t.Run("simple config", func(t *testing.T) {
		filepath, remove := writeFile(t, simpleConfig)
		defer remove()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		reloadableConfig := MustNewReloadableConfig(filepath, nil, passthroughParseFn)
		observeEvaluationTimeFNCalled := false
		reloadableConfig.SetObserveEvaluationTimeFN(func(float64) { observeEvaluationTimeFNCalled = true })
		err := reloadableConfig.Start(ctx)
		require.NoError(t, err)
		require.True(t, observeEvaluationTimeFNCalled)
		cancel()
	})
}

func TestPanicsAfterRetriesExhausted(t *testing.T) {
	t.Run("missing data on `Start`", func(t *testing.T) {
		filepath, remove := writeFile(t, importingConfig)
		defer remove()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		reloadableConfig := MustNewReloadableConfig(filepath, nil, passthroughParseFn)
		err := reloadableConfig.Start(ctx)
		require.Error(t, err)
	})

	t.Run("reload routine panics", func(t *testing.T) {
		filepath, remove := writeFile(t, importingConfig)
		defer remove()
		reloadableConfig := MustNewReloadableConfig(filepath, nil, passthroughParseFn)
		reloadableConfig.publishReloadSignal() // Simulate a reload.
		require.Panics(t, func() { reloadableConfig.reloadConfig(context.Background()) })
	})
}
