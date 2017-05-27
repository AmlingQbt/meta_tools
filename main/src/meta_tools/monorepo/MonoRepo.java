package meta_tools.submanifest;

import com.google.common.base.Charsets;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import java.nio.file.Path;
import java.util.Arrays;
import meta_tools.utils.HistoryRebuilder;
import misc1.commons.concurrent.ctree.ComputationTree;
import misc1.commons.options.OptionsFragment;
import misc1.commons.options.OptionsLibrary;
import misc1.commons.options.OptionsResults;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qbt.HelpTier;
import qbt.QbtCommand;
import qbt.QbtCommandName;
import qbt.QbtCommandOptions;
import qbt.QbtUtils;
import qbt.VcsTreeDigest;
import qbt.VcsVersionDigest;
import qbt.config.QbtConfig;
import qbt.manifest.current.QbtManifest;
import qbt.options.ConfigOptionsDelegate;
import qbt.options.ParallelismOptionsDelegate;
import qbt.tip.RepoTip;
import qbt.vcs.CommitData;
import qbt.vcs.Repository;
import qbt.vcs.TreeAccessor;
import qbt.vcs.VcsRegistry;

public class MonoRepo extends QbtCommand<MonoRepo.Options> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MonoRepo.class);

    @QbtCommandName("monoRepo")
    public static interface Options extends QbtCommandOptions {
        public static final OptionsLibrary<Options> o = OptionsLibrary.of();
        public static final ConfigOptionsDelegate<Options> config = new ConfigOptionsDelegate<Options>();
        public static final OptionsFragment<Options, String> metaVcs = o.oneArg("metaVcs").transform(o.singleton("git")).helpDesc("VCS for meta");
        public static final ParallelismOptionsDelegate<Options> parallelism = new ParallelismOptionsDelegate<Options>();
        public static final OptionsFragment<Options, ImmutableList<String>> base = o.oneArg("base").helpDesc("Treat this commit, and any of its ancestors as bases.");
        public static final OptionsFragment<Options, ImmutableList<String>> inlines = o.oneArg("inline").helpDesc("Inline this commit.");
        public static final OptionsFragment<Options, ImmutableList<String>> extracts = o.oneArg("extract").helpDesc("Extract this commit.");
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
    public int run(final OptionsResults<? extends Options> options) throws Exception {
        final QbtConfig config = Options.config.getConfig(options);
        String metaVcs = options.get(Options.metaVcs);

        Path metaDir = QbtUtils.findInMeta("", null);
        final Repository metaRepository = VcsRegistry.getLocalVcs(metaVcs).getRepository(metaDir);

        final ImmutableList<VcsVersionDigest> bases;
        {
            ImmutableList.Builder<VcsVersionDigest> b = ImmutableList.builder();
            for(String base : options.get(Options.base)) {
                b.add(metaRepository.getUserSpecifiedCommit(base));
            }
            bases = b.build();
        }

        abstract class Side extends HistoryRebuilder {
            private final String sideName;

            public Side(String sideName) {
                super(metaRepository, bases, sideName);

                this.sideName = sideName;
            }
        }

        ComputationTree<?> inlineTree = new Side("inline") {
            @Override
            protected VcsVersionDigest mapBase(VcsVersionDigest base) {
                CommitData.Builder cd = metaRepository.getCommitData(base).builder();
                if(parseHeader(cd.get(CommitData.MESSAGE)) != null) {
                    throw new IllegalStateException("Commit already has header: " + base);
                }
                cd = cd.set(CommitData.PARENTS, ImmutableList.of(base));
                cd = cd.set(CommitData.MESSAGE, "(monoRepo inline)");
                cd = addHeader(cd, base);
                return metaRepository.createCommit(cd.build());
            }

            @Override
            protected VcsVersionDigest map(VcsVersionDigest commit, CommitData cd, ImmutableList<VcsVersionDigest> parents) {
                if(parseHeader(cd.get(CommitData.MESSAGE)) != null) {
                    throw new IllegalStateException("Commit already has header: " + commit);
                }

                CommitData.Builder inlined = naiveInline(cd.builder().set(CommitData.PARENTS, parents));
                CommitData.Builder naiveExtract = naiveExtract(inlined);

                if(!cd.equals(naiveExtract)) {
                    inlined = addHeader(inlined, commit);
                }

                return metaRepository.createCommit(inlined.build());
            }
        }.buildMany(options.get(Options.inlines));

        ComputationTree<?> extractTree = new Side("extract") {
            @Override
            protected VcsVersionDigest mapBase(VcsVersionDigest base) {
                return base;
            }

            @Override
            protected VcsVersionDigest map(VcsVersionDigest next, CommitData cd, ImmutableList<VcsVersionDigest> parents) {
                Pair<String, VcsVersionDigest> header = parseHeader(cd.get(CommitData.MESSAGE));
                if(header == null) {
                    return metaRepository.createCommit(naiveExtract(cd.builder().set(CommitData.PARENTS, parents)).build());
                }
                // TODO: validate header
                return header.getRight();
            }
        }.buildMany(options.get(Options.extracts));

        Options.parallelism.getResult(options, false).runComputationTree(ComputationTree.pair(inlineTree, extractTree));
        return 0;
    }

    private static byte[] linesToBytes(Iterable<String> lines) {
        StringBuilder sb = new StringBuilder();
        for(String line : lines) {
            sb.append(line);
            sb.append('\n');
        }
        return sb.toString().getBytes(Charsets.UTF_8);
    }

    private static CommitData.Builder addHeader(CommitData.Builder cd, VcsVersionDigest commit) {
    }

    private static Pair<String, VcsVersionDigest> parseHeader(String message) {
    }

    private static CommitData.Builder naiveInline(CommitData.Builder cd) {
    }

    private static CommitData.Builder naiveExtract(CommitData.Builder cd) {
    }
}
