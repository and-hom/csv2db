package main

import (
	"github.com/and-hom/csv2db/common"
	log "github.com/Sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"os"
	"os/user"
	"path/filepath"
	"io/ioutil"
	"reflect"
	"strings"
)

const postgres = "postgres"
const mysql = "mysql"
const oracle = "oracle"

const MODE_CREATE = "create"
const MODE_DELETE_ALL = "delete-all"
const MODE_TRUNCATE = "truncate"
const MODE_DROP_AND_CREATE = "drop-and-create"
const MODE_TABLE_AS_IS = "as-is"

var modes = []string{
	MODE_CREATE,
	MODE_DELETE_ALL,
	MODE_TRUNCATE,
	MODE_DROP_AND_CREATE,
	MODE_TABLE_AS_IS,
}

type Config struct {
	DbUrl string

	Schema       string
	Table        string
	TableMode    TableMode

	FileName     string
	HasHeader    bool
	Delimiter    string
	Encoding     string
}

type TableMode string

func (this TableMode) CreateIfMissing() bool {
	return this == MODE_CREATE
}

func (this TableMode) DropAndCreateIfExists() bool {
	return this == MODE_DROP_AND_CREATE
}

func (this TableMode) DeletePrevious() bool {
	return this == MODE_DELETE_ALL
}

func (this TableMode) TruncatePrevious() bool {
	return this == MODE_TRUNCATE
}

func (this Config) String() string {
	return common.ObjectToJson(this, true)
}

func (this Config) Validate() {
	if len(this.Delimiter) > 1 {
		log.Fatalf("CSV delimiter should be a single char: %s", this.Delimiter)
	} else if (len(this.Delimiter) == 0) {
		log.Fatalf("Should set CSV delimiter")
	}
	modeOk := (string(this.TableMode) == "")
	for _, mode := range modes {
		if mode == string(this.TableMode) {
			modeOk = true
			break
		}
	}
	if !modeOk {
		log.Fatalf("Unsupported table mode %s. Available are: %s", this.TableMode, strings.Join(modes, ", "))
	}
}

func (this *Config)FillMissingFromPreset(preset Config) {
	thisVal := reflect.ValueOf(this).Elem()
	presetVal := reflect.ValueOf(preset)

	for i := 0; i < thisVal.NumField(); i++ {
		thisField := thisVal.Field(i)
		presetField := presetVal.Field(i)
		if thisField.Kind() == reflect.String && thisField.String() == "" && presetField.String() != "" {
			thisField.SetString(presetField.String())
		}
	}
}

const DEFAULT_PRESET = "default"

type ConfigStorage struct {
	Presets map[string]Config
}

func (this ConfigStorage) String() string {
	return common.ObjectToJson(this, true)
}

func LoadConfigStorage() ConfigStorage {
	file, confPath, err := configFile(false)
	if err != nil {
		log.Warn("Can not load config: ", err)
		return ConfigStorage{Presets:make(map[string]Config)}
	}
	confBytes, err := ioutil.ReadAll(file)
	if err != nil {
		log.Warnf("Can not load bytes from config file %s: %v", confPath, err)
		return ConfigStorage{Presets:make(map[string]Config)}
	}
	configStorage := ConfigStorage{}
	err = yaml.Unmarshal(confBytes, &configStorage)
	if err != nil {
		log.Warnf("Can not parse yaml from config file %s: %v", confPath, err)
		return ConfigStorage{Presets:make(map[string]Config)}
	}

	if configStorage.Presets == nil {
		configStorage.Presets = make(map[string]Config)
	}
	return configStorage
}

func (this ConfigStorage) Save() {
	file, confPath, err := configFile(true)
	if err != nil {
		return
	}
	b, err := yaml.Marshal(this)
	if err != nil {
		log.Warnf("Can not serialize yaml: %v", err)
		return
	}
	_, err = file.Write(b)
	if err != nil {
		log.Warnf("Can write config file %s: %v", confPath, err)
	}
}

func configFile(create bool) (*os.File, string, error) {
	usr, err := user.Current()
	if err != nil {
		log.Warnf("Can not get current user info: %v", err)
		return nil, "", err
	}
	confPath := filepath.Join(usr.HomeDir, ".csv2db.yaml")
	var file *os.File
	if create {
		file, err = os.Create(confPath)
	} else {
		file, err = os.Open(confPath)
	}
	if err != nil {
		log.Warnf("Can not read config file %s: %v", confPath, err)
	}
	return file, confPath, err
}
