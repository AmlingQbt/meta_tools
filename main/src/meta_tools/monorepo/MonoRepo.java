package meta_tools.submanifest;

import com.google.common.collect.ImmutableList;
import java.nio.file.Path;
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
import qbt.VcsVersionDigest;
import qbt.config.QbtConfig;
import qbt.options.ConfigOptionsDelegate;
import qbt.options.ParallelismOptionsDelegate;
import qbt.vcs.CommitData;
import qbt.vcs.Repository;
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

        class Naive {
            public CommitData.Builder inline(CommitData.Builder cd) {
                // TODO
                return cd;
            }

            public CommitData.Builder extract(CommitData.Builder cd) {
                // TODO
                return cd;
            }
        }
        Naive naive = new Naive();

        abstract class Side extends HistoryRebuilder {
            private final String sideName;

            public Side(String sideName) {
                super(metaRepository, bases, sideName);

                this.sideName = sideName;
            }
        }

        Side inlineSide = new Side("inline") {
            @Override
            protected ComputationTree<VcsVersionDigest> mapBase(VcsVersionDigest base) {
                CommitData.Builder cd = metaRepository.getCommitData(base).builder();
                if(parseHeader(cd.get(CommitData.MESSAGE)) != null) {
                    throw new IllegalStateException("Commit already has header: " + base);
                }
                cd = cd.set(CommitData.PARENTS, ImmutableList.of(base));
                cd = cd.set(CommitData.MESSAGE, "(monoRepo inline)");
                cd = addHeader(cd, base);
                return ComputationTree.constant(metaRepository.createCommit(cd.build()));
            }

            @Override
            protected ComputationTree<VcsVersionDigest> map(VcsVersionDigest commit, CommitData cd, ImmutableList<VcsVersionDigest> parents) {
                if(parseHeader(cd.get(CommitData.MESSAGE)) != null) {
                    throw new IllegalStateException("Commit already has header: " + commit);
                }

                CommitData.Builder inlined = naive.inline(cd.builder().set(CommitData.PARENTS, parents));
                CommitData.Builder naiveExtract = naive.extract(inlined);

                if(!cd.equals(naiveExtract)) {
                    inlined = addHeader(inlined, commit);
                }

                return ComputationTree.constant(metaRepository.createCommit(inlined.build()));
            }
        };
        ComputationTree<?> inlineTree = inlineSide.buildMany(options.get(Options.inlines));

        ComputationTree<?> extractTree = new Side("extract") {
            @Override
            protected ComputationTree<VcsVersionDigest> mapBase(VcsVersionDigest base) {
                return ComputationTree.constant(base);
            }

            @Override
            protected ComputationTree<VcsVersionDigest> map(VcsVersionDigest commit, CommitData cd, ImmutableList<VcsVersionDigest> parents) {
                Pair<String, VcsVersionDigest> header = parseHeader(cd.get(CommitData.MESSAGE));
                if(header == null) {
                    return ComputationTree.constant(metaRepository.createCommit(naive.extract(cd.builder().set(CommitData.PARENTS, parents)).build()));
                }
                VcsVersionDigest alleged = header.getRight();
                return inlineSide.build(alleged).transform((reinlined) -> {
                    if(reinlined.equals(commit)) {
                        return alleged;
                    }
                    throw new IllegalStateException("Illegal header on: " + commit);
                });
            }
        }.buildMany(options.get(Options.extracts));

        Options.parallelism.getResult(options, false).runComputationTree(ComputationTree.pair(inlineTree, extractTree));
        return 0;
    }

    private static final String HEADER_SEP = "\n\nX-MonoRepo-Commit: ";

    private static CommitData.Builder addHeader(CommitData.Builder cd, VcsVersionDigest commit) {
        return cd.transform(CommitData.MESSAGE, (msg) -> msg + HEADER_SEP + commit.getRawDigest());
    }

    private static Pair<String, VcsVersionDigest> parseHeader(String message) {
        int i = message.lastIndexOf(HEADER_SEP);
        if(i == -1) {
            return null;
        }
        return Pair.of(message.substring(0, i), VcsVersionDigest.PARSE_FUNCTION.apply(message.substring(i + HEADER_SEP.length())));
    }
}
