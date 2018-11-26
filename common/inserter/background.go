package inserter

import (
	"github.com/and-hom/csv2db/common"
	"github.com/Sirupsen/logrus"
	"sync"
)

const QUEUE_SIZE = 4096

func Background(inserter *common.Inserter) common.Inserter {
	backgroundInserter := backgroundInserter{
		inserter:inserter,
		dataChan:make(chan []string, QUEUE_SIZE),
	}
	backgroundInserter.wg.Add(1)
	go backgroundInserter.insertLoop()
	return &backgroundInserter
}

type backgroundInserter struct {
	inserter *common.Inserter
	dataChan chan []string
	wg       sync.WaitGroup
}

func (this *backgroundInserter) insertLoop() {
	defer this.wg.Done()
	for {
		args, ok := <-this.dataChan
		if !ok {
			break
		}
		err := (*this.inserter).Add(args...)
		if err != nil {
			logrus.Fatal("Can not insert: ", err)
			return
		}
	}
	err := (*this.inserter).Close()
	if err != nil {
		logrus.Fatal("Can not close inserter: ", err)
		return
	}
}

func (this *backgroundInserter) Add(args ...string) error {
	this.dataChan <- args
	return nil
}

func (this *backgroundInserter) Close() error {
	close(this.dataChan)
	this.wg.Wait()
	return nil
}