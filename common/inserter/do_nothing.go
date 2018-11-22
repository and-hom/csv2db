package inserter

type DoNothingInserter struct {
}

func (this DoNothingInserter) Add(...string) error {
	return nil
}

func (this DoNothingInserter) Close() error {
	return nil
}
