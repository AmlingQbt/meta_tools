package meta_tools.submanifest;

import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import misc1.commons.concurrent.WorkPool;
import misc1.commons.concurrent.ctree.ComputationTree;
import misc1.commons.concurrent.ctree.ComputationTreeComputer;
import misc1.commons.options.OptionsFragment;
import misc1.commons.options.OptionsResults;
import misc1.commons.options.UnparsedOptionsFragment;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qbt.HelpTier;
import qbt.QbtCommand;
import qbt.QbtCommandName;
import qbt.QbtUtils;
import qbt.VcsVersionDigest;
import qbt.config.QbtConfig;
import qbt.vcs.CommitData;
import qbt.vcs.CommitDataUtils;
import qbt.vcs.Repository;
import qbt.vcs.VcsRegistry;

public class SubmanifestLift extends QbtCommand<SubmanifestLift.Options> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SubmanifestLift.class);

    @QbtCommandName("submanifestLift")
    public static interface Options extends SubmanifestCommonOptions {
        public static final OptionsFragment<Options, ?, ImmutableList<String>> commit = new UnparsedOptionsFragment<Options>("Commit to lift.", false, 1, 1);
    }

    @Override
    public Class<Options> getOptionsClass() {
        return Options.class;
    }

    @Override
    public HelpTier getHelpTier() {
        return HelpTier.ARCANE;
    }

    @Override
    public String getDescription() {
        return null;
    }

    @Override
    public boolean isProgrammaticOutput() {
        return true;
    }

    @Override
    public int run(OptionsResults<? extends Options> options) throws Exception {
        final QbtConfig config = SubmanifestCommonOptions.config.getConfig(options);
        String metaVcs = options.get(SubmanifestCommonOptions.metaVcs);

        Path metaDir = QbtUtils.findInMeta("", null);
        Repository metaRepository = VcsRegistry.getLocalVcs(metaVcs).getRepository(metaDir);

        Collection<VcsVersionDigest> bases = Lists.transform(options.get(SubmanifestCommonOptions.base), VcsVersionDigest.PARSE_FUNCTION);
        VcsVersionDigest commit = VcsVersionDigest.PARSE_FUNCTION.apply(Iterables.getOnlyElement(options.get(Options.commit)));
        Map<VcsVersionDigest, CommitData> revWalk = metaRepository.revWalk(bases, ImmutableList.of(commit));
        Iterable<Pair<VcsVersionDigest, CommitData>> revWalkFlat = CommitDataUtils.revWalkFlatten(revWalk, commit);

        LoadingCache<VcsVersionDigest, ComputationTree<VcsVersionDigest>> baseComputationTrees = CacheBuilder.newBuilder().build(new CacheLoader<VcsVersionDigest, ComputationTree<VcsVersionDigest>>() {
            @Override
            public ComputationTree<VcsVersionDigest> load(VcsVersionDigest base) {
                // TODO
                return null;
            }
        });
        Map<VcsVersionDigest, ComputationTree<VcsVersionDigest>> computationTrees = Maps.newHashMap();

        for(Pair<VcsVersionDigest, CommitData> e : revWalkFlat) {
            final VcsVersionDigest next = e.getKey();
            final CommitData cd = e.getValue();

            ImmutableList.Builder<ComputationTree<VcsVersionDigest>> parentComputationTreesBuilder = ImmutableList.builder();
            for(VcsVersionDigest parent : cd.parents) {
                if(revWalk.containsKey(parent)) {
                    parentComputationTreesBuilder.add(computationTrees.get(parent));
                }
                else {
                    parentComputationTreesBuilder.add(baseComputationTrees.getUnchecked(parent));
                }
            }

            ComputationTree<VcsVersionDigest> nextTree = ComputationTree.list(parentComputationTreesBuilder.build()).transform(new Function<ImmutableList<VcsVersionDigest>, VcsVersionDigest>() {
                @Override
                public VcsVersionDigest apply(ImmutableList<VcsVersionDigest> parentResults) {
                    LOGGER.debug("Processing " + next + "...");
                    // TODO
                    return null;
                }
            });

            computationTrees.put(next, nextTree);
        }

        final WorkPool workPool = Options.parallelism.getResult(options, false).createWorkPool();
        try {
            ComputationTreeComputer ctc = new ComputationTreeComputer() {
                @Override
                protected void submit(Runnable r) {
                    workPool.submit(r);
                }
            };

            VcsVersionDigest result = ctc.await(computationTrees.get(commit)).getCommute();
            System.out.println(String.valueOf(result.getRawDigest()));

            return 0;
        }
        finally {
            workPool.shutdown();
        }
    }
}
