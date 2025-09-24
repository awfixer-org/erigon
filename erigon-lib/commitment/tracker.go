package commitment

import (
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
)

type StopWatch struct {
	blockNum   uint64
	start      time.Time
	c          uint64
	cumulative time.Duration
	logger     log.Logger
}

func NewStopWatch(logger log.Logger) *StopWatch {
	return &StopWatch{
		blockNum: 0,
		start:    time.Unix(0, 0),
		logger:   logger,
	}
}

func (s *StopWatch) StartBlock(blockNum uint64) {
	if s.start.Unix() != 0 {
		panic("stop watch already started")
	}
	s.blockNum = blockNum
	s.c = 0
	s.cumulative = 0
	s.logger.Info("start block", "block", s.blockNum)
}

func (s *StopWatch) EndBlock() {
	if s.start.Unix() != 0 {
		panic("stop watch already started")
	}

	s.logger.Warn("stop block", "block", s.blockNum, "in", s.cumulative.Milliseconds(), "c", s.c)
}

func (s *StopWatch) Start() {
	if s.start.Unix() != 0 {
		panic("stop watch already started")
	}
	s.start = time.Now()
	// s.logger.Info("start stop watch", "block", s.blockNum)
}

func (s *StopWatch) Stop() {
	if s.start.Unix() == 0 {
		panic("stop watch not started")
	}

	dur := time.Since(s.start)
	s.c++
	s.cumulative += dur
	// s.logger.Info("stop stop watch", "block", s.blockNum)
	s.start = time.Unix(0, 0)
}
