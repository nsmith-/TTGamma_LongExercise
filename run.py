#!/usr/bin/env python
import argparse
import time
import math
from coffea import util, processor
from ttgamma import TTGammaProcessor
from ttgamma.utils.fileSet_2016_LZ4 import fileSet_2016, fileSet_Data_2016


FILESETS = {
    'mc_2016': fileSet_2016,
    'data_2016': fileSet_Data_2016,
}


def setup_fileset(args):
    fileset_in = FILESETS[args.fileset]

    if args.split is not None:
        # Split the fileset into even chunks
        njob, ijob = args.split
        all_files = sum((list(files) for files in fileset_in.values()), [])
        files_per_job = math.ceil(len(all_files) / njob)
        files_this_job = all_files[ijob*files_per_job:(ijob+1)*files_per_job]
        fileset = {}
        for dataset, files_in in fileset_in.items():
            files = [f for f in files_in if f in files_this_job]
            if len(files) == 0:
                continue
            fileset[dataset] = files
    else:
        fileset = fileset_in

    if args.debug:
        print("Fileset for this job:", fileset)

    return fileset


def run(args):
    tstart = time.time()
    mcEventYields = {'DYjetsM10to50_2016': 35114961.0, 'DYjetsM50_2016': 146280395.0, 'GJets_HT40To100_2016': 9326139.0, 'GJets_HT100To200_2016': 10104155.0, 'GJets_HT200To400_2016': 20527506.0, 'GJets_HT400To600_2016': 5060070.0, 'GJets_HT600ToInf_2016': 5080857.0, 'QCD_Pt20to30_Ele_2016': 9241500.0, 'QCD_Pt30to50_Ele_2016': 11508842.0, 'QCD_Pt50to80_Ele_2016': 45789059.0, 'QCD_Pt80to120_Ele_2016': 77800204.0, 'QCD_Pt120to170_Ele_2016': 75367655.0, 'QCD_Pt170to300_Ele_2016': 11105095.0, 'QCD_Pt300toInf_Ele_2016': 7090318.0, 'QCD_Pt20to30_Mu_2016': 31878740.0, 'QCD_Pt30to50_Mu_2016': 29936360.0, 'QCD_Pt50to80_Mu_2016': 19662175.0, 'QCD_Pt80to120_Mu_2016': 23686772.0, 'QCD_Pt120to170_Mu_2016': 7897731.0, 'QCD_Pt170to300_Mu_2016': 17350231.0, 'QCD_Pt300to470_Mu_2016': 49005976.0, 'QCD_Pt470to600_Mu_2016': 19489276.0, 'QCD_Pt600to800_Mu_2016': 9981311.0, 'QCD_Pt800to1000_Mu_2016': 19940747.0, 'QCD_Pt1000toInf_Mu_2016': 13608903.0, 'ST_s_channel_2016': 6137801.0, 'ST_tW_channel_2016': 4945734.0, 'ST_tbarW_channel_2016': 4942374.0, 'ST_tbar_channel_2016': 17780700.0, 'ST_t_channel_2016': 31848000.0, 'TTGamma_Dilepton_2016': 5728644.0, 'TTGamma_Hadronic_2016': 5635346.0, 'TTGamma_SingleLept_2016': 10991612.0, 'TTWtoLNu_2016': 2716249.0, 'TTWtoQQ_2016': 430310.0, 'TTZtoLL_2016': 6420825.0, 'TTbarPowheg_Dilepton_2016': 67339946.0, 'TTbarPowheg_Hadronic_2016': 67963984.0, 'TTbarPowheg_Semilept_2016': 106438920.0, 'W1jets_2016': 45283121.0, 'W2jets_2016': 60438768.0, 'W3jets_2016': 59300029.0, 'W4jets_2016': 29941394.0, 'WGamma_01J_5f_2016': 6103817.0, 'ZGamma_01J_5f_lowMass_2016': 9696539.0, 'WW_2016': 7982180.0, 'WZ_2016': 3997571.0, 'ZZ_2016': 1988098.0}

    outputData = processor.run_uproot_job(
        setup_fileset(args),
        treename='Events',
        processor_instance=TTGammaProcessor(mcEventYields=mcEventYields),
        executor=processor.iterative_executor,
        executor_args={
            'flatten': True,
            'status': args.debug,
        },
        chunksize=args.chunksize,
        maxchunks=args.maxchunks,
    )

    util.save(outputData, args.output)

    elapsed = time.time() - tstart
    if args.debug:
        print("Total time: %.1f seconds"%elapsed)
        print("Total rate: %.1f events / second"%(outputData['EventCount'].value/elapsed))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run ttgamma processor')
    parser.add_argument('--maxchunks', type=int, default=None, metavar='N', help='Limit to the first N chunks (default: %(default)s)')
    parser.add_argument('--chunksize', type=int, default=200000, metavar='N', help='Chunk size (default: %(default)s)')
    parser.add_argument('--fileset', type=str, choices=FILESETS.keys(), default='mc_2016', help='Which input file set to use (default: %(default)s)')
    parser.add_argument('--output', default='output.coffea', help='Output filename (default: %(default)s)')
    parser.add_argument('--split', type=int, help="Split input file list and process subsection. IJOB is zero-indexed", nargs=2, metavar=('NJOBS', 'IJOB'))
    parser.add_argument('--debug', '-v', action='store_true', help="Be more verbose")
    args = parser.parse_args()

    run(args)
