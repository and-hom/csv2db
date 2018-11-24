package main

import (
	"github.com/schollz/progressbar"
	"fmt"
	"time"
)

type ProgressBar struct {
	quit         chan bool
	progressFunc func() int64
	size         int64
}

func InitProgressBar(progressFunc func() int64, size int64) ProgressBar {
	return ProgressBar{
		quit:make(chan bool, 1),
		progressFunc:progressFunc,
		size: size,
	}
}

func (this ProgressBar) Start() {
	go this.progressBarLoop()
}

func (this ProgressBar) Stop() {
	this.quit <- true
	defer close(this.quit)
}

func (this ProgressBar)progressBarLoop() {
	bar := progressbar.NewOptions(int(this.size),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetDescription("Bytes transfered:"))
	prevSize := 0

	for {
		select {
		default:
			newSize := int(this.progressFunc())
			bar.Add(newSize - prevSize)
			prevSize = newSize
		case <-this.quit:
			bar.Finish()
			fmt.Println()
			return
		}
	}
	time.Sleep(100 * time.Millisecond)
}
