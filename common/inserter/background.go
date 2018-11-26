package inserter

import (
	"github.com/and-hom/csv2db/common"
	"github.com/Sirupsen/logrus"
	"sync"
)

const QUEUE_SIZE = 512

func Background(inserter *common.Inserter) common.Inserter {
	backgroundInserter := backgroundInserter{
		inserter:inserter,
		dataChan:make(chan []string, QUEUE_SIZE),
	}
	go backgroundInserter.insertLoop()
	return &backgroundInserter
}

type backgroundInserter struct {
	inserter *common.Inserter
	dataChan chan []string
	wg       sync.WaitGroup
}

func (this *backgroundInserter) insertLoop() {
	this.wg.Add(1)
	i:=1
	for {
		args, ok := <-this.dataChan
		if !ok {
			break
		}
		if i%100 == 0 {
			logrus.Debug(len(this.dataChan))
		}
		i+=1
		err := (*this.inserter).Add(args...)
		if err != nil {
			this.wg.Done()
			logrus.Fatal("Can not insert: ", err)
			return
		}
	}
	err := (*this.inserter).Close()
	if err != nil {
		this.wg.Done()
		logrus.Fatal("Can not close inserter: ", err)
		return
	} else {
		this.wg.Done()
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