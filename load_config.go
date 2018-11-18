package main

import (
	"strings"
	"gopkg.in/urfave/cli.v1"
	log "github.com/Sirupsen/logrus"
)

func LoadConfig(c *cli.Context) Config {
	loadedConfig := loadFromCliArgs(c)
	configStorage := LoadConfigStorage()
	preset := getPreset(c, configStorage)

	loadedConfig.FillMissingFromPreset(preset)

	setPreset(c, configStorage, loadedConfig)
	configStorage.Save()

	return loadedConfig
}

func loadFromCliArgs(c *cli.Context) Config {
	tableParts := strings.Split(c.String(flagName(TABLE_FLAG)), ".")
	schemaName := ""
	if len(tableParts) > 1 {
		schemaName = tableParts[0]
	}
	cliConfig := Config{
		DbType:DbType(c.String(flagName(DB_TYPE_FLAG))),
		DbConnString:c.String(flagName(CONN_STRING_FLAG)),

		Schema:schemaName,
		Table:tableParts[len(tableParts) - 1],
		TableMode:TableMode(c.String(flagName(TABLE_MODE_FLAG))),

		FileName : c.String(flagName(INPUT_FILE_FLAG)),
		HasHeader : c.Bool(flagName(HEADER_FLAG)),
		Delimiter : c.String(flagName(DELIMITER_FLAG)),
		Encoding : c.String(flagName(ENCODING_FLAG)),
	}
	cliConfig.Validate()
	return cliConfig
}

func getPreset(c *cli.Context, configStorage ConfigStorage) Config {
	presetName := c.String(PRESET_FLAG)
	if presetName=="" {
		presetName = DEFAULT_PRESET
	}
	preset, found := configStorage.Presets[presetName]
	if !found {
		if presetName != DEFAULT_PRESET {
			log.Warn("No preset found by key %s", presetName)
		}
		return Config{}
	}
	return preset
}


func setPreset(c *cli.Context, configStorage ConfigStorage, preset Config) {
	storePreset := c.String(flagName(STORE_PRESET_FLAG))
	if storePreset != "" {
		configStorage.Presets[storePreset] = preset
	}
}
