package main

import (
	"github.com/xo/dburl"
	"fmt"
	"bufio"
	"os"
	"github.com/Sirupsen/logrus"
	"net/url"
	"golang.org/x/crypto/ssh/terminal"
	"syscall"
	"strings"
)

var AUTH AuthProvider = Auth{Providers:[]AuthProvider{
	UrlContainsAuthInfo{},
	AskForMissing{},
}}

func initializeCredentialsIfMissing(dbUrl *dburl.URL) {
	authorized := AUTH.InitializeUserInfo(dbUrl)
	if !authorized {
		logrus.Warnf("No authorization info - insert may fail")
	} else {
		d, err := dburl.Parse(dbUrl.String())
		if err != nil {
			logrus.Error(err)
		}
		dbUrl.DSN = d.DSN
	}
}

type AuthProvider interface {
	InitializeUserInfo(url *dburl.URL) bool
}

type UrlContainsAuthInfo struct {
}

func (this UrlContainsAuthInfo) InitializeUserInfo(dbUrl *dburl.URL) bool {
	_, passwordSet := dbUrl.User.Password()
	return dbUrl.User.Username() != "" && passwordSet
}

type AskForMissing struct {
}

func (this AskForMissing) InitializeUserInfo(dbUrl *dburl.URL) bool {
	reader := bufio.NewReader(os.Stdin)
	var err error

	userName := dbUrl.User.Username()
	if userName == "" {
		fmt.Print("Login: ")
		userName, err = reader.ReadString('\n')
		if err != nil {
			logrus.Fatal("Failed to read username ", err)
			return false
		}
		userName = strings.TrimSpace(userName)
	}

	password, passwordSet := dbUrl.User.Password()
	if !passwordSet {
		fmt.Print("Password: ")
		p, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			logrus.Fatal("Failed to read password ", err)
			return false
		}
		password = string(p)
	}
	dbUrl.User = url.UserPassword(userName, password)
	return true
}

type Auth struct {
	Providers []AuthProvider
}

func (this Auth) InitializeUserInfo(url *dburl.URL) bool {
	for _, p := range this.Providers {
		if p.InitializeUserInfo(url) {
			return true
		}
	}
	return false
}