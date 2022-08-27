package fs

type Move struct {
	SourceBucket string
	SourceKey    string
	DestBucket   string
	DestKey      string
}

func NewMove(source, dest Position) Move {
	return Move{
		SourceBucket: source.Bucket,
		SourceKey:    source.Key,
		DestBucket:   dest.Bucket,
		DestKey:      dest.Key,
	}
}
