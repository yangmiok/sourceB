package storage

import (
	"context"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/lotus/lib/ipfsunion/multiminer"  //ipfsunion add
        "github.com/filecoin-project/specs-actors/actors/builtin" //ipfsunion add
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	sectorstorage "github.com/filecoin-project/lotus/extern/sector-storage"
	"github.com/filecoin-project/lotus/node/config"

	"go.opencensus.io/trace"
)

const StartConfidence = 4 // TODO: config

var FakeWPostChannel = make(chan []int) //ipfsunion add

type DeadlineContext struct {
	activeDeadline *miner.DeadlineInfo
	abort          context.CancelFunc
}

type WindowPoStScheduler struct {
	api              storageMinerApi
	feeCfg           config.MinerFeeConfig
	prover           storage.Prover
	faultTracker     sectorstorage.FaultTracker
	proofType        abi.RegisteredPoStProof
	partitionSectors uint64

	actor  address.Address
	worker address.Address

	cur *types.TipSet

	// if a post is in progress, this indicates for which ElectionPeriodStart
	oddDeadLineContext DeadlineContext
	pairDeadLineContext DeadlineContext

	//failed abi.ChainEpoch // eps
	//failLk sync.Mutex
	cachedProofs *miner.SubmitWindowedPoStParams
	//cachedProofsOpen abi.ChainEpoch
	//tipsetKey  types.TipSetKey
}

func NewWindowedPoStScheduler(api storageMinerApi, fc config.MinerFeeConfig, sb storage.Prover, ft sectorstorage.FaultTracker, actor address.Address, worker address.Address) (*WindowPoStScheduler, error) {
	mi, err := api.StateMinerInfo(context.TODO(), actor, types.EmptyTSK)
	if err != nil {
		return nil, xerrors.Errorf("getting sector size: %w", err)
	}

	rt, err := mi.SealProofType.RegisteredWindowPoStProof()
	if err != nil {
		return nil, err
	}

	return &WindowPoStScheduler{
		api:              api,
		feeCfg:           fc,
		prover:           sb,
		faultTracker:     ft,
		proofType:        rt,
		partitionSectors: mi.WindowPoStPartitionSectors,

		actor:  actor,
		worker: worker,
		cachedProofs:nil,
		//cachedProofsOpen:0,
	}, nil
}

func deadlineEquals(a, b *miner.DeadlineInfo) bool {
	if a == nil || b == nil {
		return b == a
	}

	return a.PeriodStart == b.PeriodStart && a.Index == b.Index && a.Challenge == b.Challenge
}

func (s *WindowPoStScheduler) Run(ctx context.Context) {

	/* ipfsunion add begin */
	if !multiminer.WindowPost {
		log.Infof("(MultiMiner) WindowPost is false, WindowPoStScheduler return")
		return
	}
	/* ipfsunion add end */
        defer s.abortActivePoSt(0)
	defer s.abortActivePoSt(1)

	var notifs <-chan []*api.HeadChange
	var err error
	var gotCur bool

	// not fine to panic after this point
	for {
		if notifs == nil {
			notifs, err = s.api.ChainNotify(ctx)
			if err != nil {
				log.Errorf("ChainNotify error: %+v", err)

				build.Clock.Sleep(10 * time.Second)
				continue
			}

			gotCur = false
		}

		select {
		case changes, ok := <-notifs:
			if !ok {
				log.Warn("WindowPoStScheduler notifs channel closed")
				notifs = nil
				continue
			}

			if !gotCur {
				if len(changes) != 1 {
					log.Errorf("expected first notif to have len = 1")
					continue
				}
				if changes[0].Type != store.HCCurrent {
					log.Errorf("expected first notif to tell current ts")
					continue
				}

				if err := s.update(ctx, changes[0].Val); err != nil {
					log.Errorf("%+v", err)
				}

				gotCur = true
				continue
			}

			ctx, span := trace.StartSpan(ctx, "WindowPoStScheduler.headChange")

			var lowest, highest *types.TipSet = s.cur, nil

			for _, change := range changes {
				if change.Val == nil {
					log.Errorf("change.Val was nil")
				}
				switch change.Type {
				case store.HCRevert:
					lowest = change.Val
				case store.HCApply:
					highest = change.Val
				}
			}

			if err := s.revert(ctx, lowest); err != nil {
				log.Error("handling head reverts in windowPost sched: %+v", err)
			}
			if err := s.update(ctx, highest); err != nil {
				log.Error("handling head updates in windowPost sched: %+v", err)
			}

			span.End()
		case <-ctx.Done():
			return
		}
	}
}

/*ipfsunion begin*/
func (s *WindowPoStScheduler) FakeWPoStRun(ctx context.Context) {
	log.Infof("fakeWPoSt FakeWPoStRun Enter")

	for {
		select {
		case hs := <-FakeWPostChannel:
			log.Infof("fakeWPoSt FakeWPoStRun heights:%v", hs)
			go func(ctx context.Context, heights []int) {
				for _, h := range heights {
					if err := s.FakeWPoSt(ctx, h); err != nil {
						log.Errorf("fakeWPoSt FakeWPoStRun FakeWPoSt height %v | %v", h, err)
					}
				}
			}(ctx, hs)
		}
	}
}

/*ipfsunion end*/

/*ipfsunion begin*/
func (s *WindowPoStScheduler) FakeWPoSt(ctx context.Context, height int) error {
	//log.Infof("fakeWPoSt FakeWPoSt heights:%v Enter", height)
	//defer log.Infof("fakeWPoSt FakeWPoSt heights:%v Out", height)
	//
	//ts, err := s.api.ChainGetTipSetByHeight(ctx, abi.ChainEpoch(height), types.EmptyTSK)
	//if err != nil {
	//	return xerrors.Errorf("fakeWPoSt ChainGetTipSetByHeight %v | %v ", height, err)
	//}
	//
	//deadline, err := s.api.StateMinerProvingDeadline(ctx, s.actor, ts.Key())
	//if err != nil {
	//	return xerrors.Errorf("fakeWPoSt StateMinerProvingDeadline %v | %v ", height, err)
	//}
	//
	//log.Infof("fakeWPoSt deadline %+v ts %+v | %v", deadline, ts, err)
	//_, err = s.fakeRunPost(ctx, *deadline, ts)
	//log.Warnf("fakeWPoSt deadline %+v runPost | %v", deadline, err)
	//if err != nil {
	//	return xerrors.Errorf("fakeWPoSt runPost | %v", err)
	//}
	return nil
}

//func (s *WindowPoStScheduler) fakeRunPost(ctx context.Context, di miner.DeadlineInfo, ts *types.TipSet) (*miner.SubmitWindowedPoStParams, error) {
//	ctx, span := trace.StartSpan(ctx, "storage.runPost")
//	defer span.End()
//
//	buf := new(bytes.Buffer)
//	if err := s.actor.MarshalCBOR(buf); err != nil {
//		return nil, xerrors.Errorf("fakeWPoSt fakeRunPost di:%+v failed to marshal address to cbor: %w", di, err)
//	}
//
//	rand, err := s.api.ChainGetRandomnessFromBeacon(ctx, ts.Key(), crypto.DomainSeparationTag_WindowedPoStChallengeSeed, di.Challenge, buf.Bytes())
//	if err != nil {
//		return nil, xerrors.Errorf("fakeWPoSt fakeRunPost failed to get chain randomness for windowPost (ts=%d; deadline=%d): %w", ts.Height(), di, err)
//	}
//
//	partitions, err := s.api.StateMinerPartitions(ctx, s.actor, di.Index, ts.Key())
//	if err != nil {
//		return nil, xerrors.Errorf("fakeRunPost di:%+v getting partitions: %w", di, err)
//	}
//
//	params := &miner.SubmitWindowedPoStParams{
//		Deadline:   di.Index,
//		Partitions: make([]miner.PoStPartition, 0, len(partitions)),
//		Proofs:     nil,
//	}
//
//	var sinfos []abi.SectorInfo
//	sidToPart := map[abi.SectorNumber]uint64{}
//	skipCount := uint64(0)
//
//	log.Infof("fakeWPoSt fakeRunPost partitions count %v ,di:%+v", len(partitions), di)
//
//	for partIdx, partition := range partitions {
//		// TODO: Can do this in parallel
//		toProve, err := partition.LiveSectors()
//		if err != nil {
//			return nil, xerrors.Errorf("fakeWPoSt fakeRunPost getting active sectors: %w ,partitionIdx:%v, di: %+v", err, partIdx, di)
//		}
//
//		//toProve, err = bitfield.MergeBitFields(toProve, partition.Recoveries)
//		if err != nil {
//			return nil, xerrors.Errorf("fakeWPoSt fakeRunPost adding recoveries to set of sectors to prove: %w ,partitionIdx:%v, di: %+v", err, partIdx, di)
//		}
//
//		good, err := s.checkSectors(ctx, toProve)
//		if err != nil {
//			return nil, xerrors.Errorf("fakeWPoSt fakeRunPost checking sectors to skip: %w ,partitionIdx:%v, di: %+v", err, partIdx, di)
//		}
//
//		gc, err := good.Count()
//		log.Infof("fakeWPoSt fakeRunPost good sectors %v err %v ,partitionIdx:%v, di: %+v", gc, err, partIdx, di)
//
//		skipped, err := bitfield.SubtractBitField(toProve, good)
//		if err != nil {
//			return nil, xerrors.Errorf("fakeWPoSt fakeRunPost toProve - good: %w ,partitionIdx:%v, di: %+v", err, partIdx, di)
//		}
//
//		sc, err := skipped.Count()
//		if err != nil {
//			return nil, xerrors.Errorf("fakeWPoSt fakeRunPost getting skipped sector count: %w ,partitionIdx:%v, di: %+v", err, partIdx, di)
//		}
//		log.Infof("fakeWPoSt fakeRunPost skipped count:%v, partitionIdx: %v, di: %+v", sc, partIdx, di)
//
//		if sc > 0 {
//			skippedSectors, err := s.sectorInfo(ctx, skipped, ts)
//			if err != nil {
//				log.Errorf("fakeWPoSt fakeRunPost skipped sectorInfo error: %v, partitionIdx:%v, di: %+v", err, partIdx, di)
//			} else {
//				skippedIds := make([]abi.SectorNumber, 0, sc)
//				for _, s := range skippedSectors {
//					skippedIds = append(skippedIds, s.SectorNumber)
//				}
//				log.Infof("fakeWPoSt fakeRunPost skippedIds:%v, partitionIdx:%v, di: %+v", skippedIds, partIdx, di)
//			}
//		}
//
//		skipCount += sc
//
//		log.Infof("fakeWPoSt fakeRunPost skipped count total %v, partitionIdx:%v, di: %+v", skipCount, partIdx, di)
//
//		ssi, err := s.sectorInfo(ctx, good, ts)
//		if err != nil {
//			return nil, xerrors.Errorf("fakeWPoSt fakeRunPost getting sorted sector info: %w, partitionIdx:%v, di: %+v", err, partIdx, di)
//		}
//
//		if len(ssi) == 0 {
//			log.Warnf("fakeWPoSt fakeRunPost good sectors %v err %v, partitionIdx:%v, di: %+v", gc, err, partIdx, di)
//			continue
//		}
//
//		sinfos = append(sinfos, ssi...)
//		for _, si := range ssi {
//			sidToPart[si.SectorNumber] = uint64(partIdx)
//		}
//
//		params.Partitions = append(params.Partitions, miner.PoStPartition{
//			Index:   uint64(partIdx),
//			Skipped: skipped,
//		})
//	}
//
//	log.Infow("fakeWPoSt fakeRunPost running windowPost",
//		"chain-random", rand,
//		"deadline", di,
//		"height", ts.Height(),
//		"skipped", skipCount,
//		"params", params)
//
//	if len(sinfos) == 0 {
//		// nothing to prove..
//		log.Warnf("fakeWPoSt fakeRunPost No partitions to prove ,di:%+v", di)
//		return nil, errNoPartitions
//	}
//
//	tsStart := build.Clock.Now()
//
//	log.Infow("fakeWPoSt fakeRunPost generating windowPost", "sectors", len(sinfos), " ,di:", di)
//
//	mid, err := address.IDFromAddress(s.actor)
//	if err != nil {
//		return nil, err
//	}
//
//	postOut, postSkipped, err := s.prover.GenerateWindowPoSt(ctx, abi.ActorID(mid), sinfos, abi.PoStRandomness(rand))
//	if err != nil {
//		return nil, xerrors.Errorf("fakeWPoSt fakeRunPost running post failed: %w ,di:%+v", err, di)
//	}
//
//	if len(postOut) == 0 {
//		return nil, xerrors.Errorf("fakeWPoSt fakeRunPost received proofs back from generate window post ,di:%+v", di)
//	}
//
//	params.Proofs = postOut
//
//	for _, sector := range postSkipped {
//		params.Partitions[sidToPart[sector.Number]].Skipped.Set(uint64(sector.Number))
//	}
//
//	elapsed := time.Since(tsStart)
//
//	commEpoch := di.Open
//	commRand, err := s.api.ChainGetRandomnessFromTickets(ctx, ts.Key(), crypto.DomainSeparationTag_PoStChainCommit, commEpoch, nil)
//	if err != nil {
//		return nil, xerrors.Errorf("fakeWPoSt fakeRunPost failed to get chain randomness for windowPost (ts=%d; deadline=%d): %w", ts.Height(), di, err)
//	}
//	params.ChainCommitEpoch = commEpoch
//	params.ChainCommitRand = commRand
//
//	log.Infow("fakeWPoSt fakeRunPost submitting window PoSt", "elapsed", elapsed, " ,di:", di)
//
//	return params, nil
//}

/*ipfsunion end*/

func (s *WindowPoStScheduler) revert(ctx context.Context, newLowest *types.TipSet) error {
	if s.cur == newLowest {
		return nil
	}
	s.cur = newLowest

	newDeadline, err := s.api.StateMinerProvingDeadline(ctx, s.actor, newLowest.Key())
	if err != nil {
		return err
	}

	activeDataline := s.getActiveDatalineContext(newDeadline.Index)
	if !deadlineEquals(activeDataline.activeDeadline, newDeadline) {
		s.abortActivePoSt(activeDataline.activeDeadline.Index)
	}

	return nil
}

func (s *WindowPoStScheduler) update(ctx context.Context, new *types.TipSet) error {
	if new == nil {
		return xerrors.Errorf("no new tipset in WindowPoStScheduler.update")
	}

	di, err := s.api.StateMinerProvingDeadline(ctx, s.actor, new.Key())
	if err != nil {
		return err
	}

	log.Info("di........", di);
	if s.cachedProofs != nil {
		//means not first window
		if di.CurrentEpoch - di.Challenge > builtin.EpochsInHour /2 {
			if di.CurrentEpoch < di.Open + builtin.EpochsInHour / 2 {
				log.Info("can NOT submit proofs yet2", di)
				return nil
			}
		} else if di.CurrentEpoch < di.Open {
			log.Info("can NOT submit proofs yet", di)
			return nil
		}

		s.abortActivePoSt(di.Index + 1)

		commEpoch := di.Open//first cachedpoen, others di.open
		commRand, err := s.api.ChainGetRandomnessFromTickets(ctx, new.Key(), crypto.DomainSeparationTag_PoStChainCommit, commEpoch, nil)
		if err != nil {
			if err != nil {
				log.Error("!!!!!!!!!22",err, di.Open)
				return err
			}
		}
		s.cachedProofs.ChainCommitEpoch = commEpoch
		s.cachedProofs.ChainCommitRand = commRand

		log.Info("start to submitpost()")
		if err := s.submitPost(ctx, s.cachedProofs); err != nil {
			log.Errorf("submitPost failed: %+v", err)
			s.failPost(di)
			s.cachedProofs = nil
			return nil
		}
		s.cachedProofs = nil
		//s.cachedProofsOpen = 0
		return nil
	}

	/*
	if deadlineEquals(s.activeDeadline, di) {
		return nil // already working on this deadline
	} */
	if !di.PeriodStarted() {
		return nil // not proving anything yet
	}

	// TODO: wait for di.Challenge here, will give us ~10min more to compute windowpost
	//  (Need to get correct deadline above, which is tricky)
	//if di.Challenge+20 > new.Height() {

	if di.CurrentEpoch < di.Challenge {
		log.Info("not starting windowPost yet, waiting for startconfidence", di.Challenge, di.Challenge+StartConfidence, new.Height())
		return nil
	}
	if di.CurrentEpoch == di.PeriodStart {
		log.Infof("at %d, doPost for P %d, dd %d", new.Height(), di.PeriodStart, di.Index)
		//newdi := miner.NewDeadlineInfo(di.PeriodStart, (di.Index + 1)%48, di.Open + builtin.EpochsInHour / 2);
		s.doPost(ctx, di, new)
		return nil
	}

	if di.CurrentEpoch == di.Challenge + builtin.EpochsInHour/2 {
		newdi := miner.NewDeadlineInfo(di.PeriodStart, (di.Index + 1)%48, di.Open + builtin.EpochsInHour / 2);
		log.Infof("at %d, doPost for P %d, dd %d", new.Height(), newdi.PeriodStart, newdi.Index)
		s.doPost(ctx, newdi, new)
	}
	//means anyway we missed this window
	//init it
	/*ipfsunion_end*/

	/*s.failLk.Lock()
	if s.failed > 0 {
		s.failed = 0
		s.activeEPS = 0
	}
	s.failLk.Unlock()*/


	return nil
}

func (s *WindowPoStScheduler) abortActivePoSt(index uint64) {
	activeDataline := s.getActiveDatalineContext(index)
	if activeDataline.activeDeadline == nil {
		return // noop
	}

	if activeDataline.abort != nil {
		activeDataline.abort()
	}

	log.Warnf("Aborting Window PoSt (Deadline: %+v)", activeDataline.activeDeadline)

	activeDataline.activeDeadline = nil
	activeDataline.abort = nil
}
