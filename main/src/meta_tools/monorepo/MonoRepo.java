package meta_tools.monorepo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import meta_tools.submanifest.Submanifest;
import meta_tools.utils.HistoryRebuilder;
import misc1.commons.Either;
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
import qbt.manifest.current.PackageManifest;
import qbt.manifest.current.PackageMetadata;
import qbt.manifest.current.QbtManifest;
import qbt.manifest.current.RepoManifest;
import qbt.options.ConfigOptionsDelegate;
import qbt.options.ParallelismOptionsDelegate;
import qbt.options.RepoActionOptionsDelegate;
import qbt.repo.PinnedRepoAccessor;
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
        public static final RepoActionOptionsDelegate<Options> repos = new RepoActionOptionsDelegate<Options>(RepoActionOptionsDelegate.NoArgsBehaviour.THROW);
        public static final OptionsFragment<Options, String> inlinedRepoName = o.oneArg("inlinedRepoName").transform(o.singleton("inlined"));
        public static final OptionsFragment<Options, String> inlinedPrefix = o.oneArg("inlinedPrefix").transform(o.singleton("inlined"));
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

        String inlinedRepoName = options.get(Options.inlinedRepoName);
        String inlinedPrefix = options.get(Options.inlinedPrefix);
        class Naive {
            public CommitData.Builder inline(CommitData.Builder cd) {
                VcsTreeDigest tree = cd.get(CommitData.TREE);
                QbtManifest manifest = config.manifestParser.parse(ImmutableList.copyOf(metaRepository.showFile(tree, "qbt-manifest")));
                Collection<RepoTip> repos = Options.repos.getRepos(config, manifest, options);

                QbtManifest.Builder newManifest = manifest.builder();
                TreeAccessor newTree = metaRepository.getTreeAccessor(tree);
                for(RepoTip repo : repos) {
                    newManifest = newManifest.without(repo);
                    RepoManifest repoManifest = manifest.repos.get(repo);

                    String inlinedRepoPrefix = combinePrefix(inlinedPrefix, repo.toString());

                    for(Map.Entry<String, PackageManifest> e : repoManifest.packages.entrySet()) {
                        PackageManifest.Builder newPackageManifest = e.getValue().builder();
                        newPackageManifest = newPackageManifest.transform(PackageManifest.METADATA, (pm) -> pm.transform(PackageMetadata.PREFIX, (prefix) -> combinePrefix(inlinedRepoPrefix, prefix)));

                        RepoTip inlinedTip = RepoTip.TYPE.of(inlinedRepoName, repo.tip);
                        RepoManifest.Builder inlinedManifest = newManifest.get(inlinedTip);
                        if(inlinedManifest == null) {
                            inlinedManifest = RepoManifest.TYPE.builder();
                            inlinedManifest = inlinedManifest.set(RepoManifest.VERSION, Optional.empty());
                        }
                        PackageManifest.Builder newPackageManifestFinal = newPackageManifest;
                        inlinedManifest = inlinedManifest.transform(RepoManifest.PACKAGES, (rmp) -> rmp.with(e.getKey(), newPackageManifestFinal));
                        newManifest = newManifest.with(inlinedTip, inlinedManifest);
                    }

                    VcsVersionDigest repoVersion = repoManifest.get(RepoManifest.VERSION).get();
                    PinnedRepoAccessor pinnedRepoAccessor = config.localPinsRepo.requirePin(repo, repoVersion);
                    pinnedRepoAccessor.findCommit(metaRepository.getRoot());

                    TreeAccessor repoTree = metaRepository.getTreeAccessor(metaRepository.getSubtree(repoVersion, ""));
                    if(repoTree.isEmpty()) {
                        throw new IllegalStateException("Cannot inline empty repo: " + repo);
                    }

                    newTree = newTree.replace(inlinedRepoPrefix, repoTree);
                }

                newTree = newTree.replace("qbt-manifest", Submanifest.linesToBytes(config.manifestParser.deparse(newManifest.build())));
                cd = cd.set(CommitData.TREE, newTree.getDigest());

                return cd;
            }

            public CommitData.Builder extract(CommitData.Builder cd) {
                VcsTreeDigest tree = cd.get(CommitData.TREE);
                QbtManifest manifest = config.manifestParser.parse(ImmutableList.copyOf(metaRepository.showFile(tree, "qbt-manifest")));

                TreeAccessor newTree = metaRepository.getTreeAccessor(tree);
                Either<TreeAccessor, byte[]> inlinedTreeEither = newTree.get(inlinedPrefix);
                TreeAccessor inlinedTree = inlinedTreeEither == null ? null : inlinedTreeEither.leftOrNull();
                if(inlinedTree == null) {
                    inlinedTree = metaRepository.getEmptyTreeAccessor();
                }
                newTree = newTree.remove(inlinedPrefix);

                ImmutableSet.Builder<RepoTip> reposBuilder = ImmutableSet.builder();
                for(String repoString : inlinedTree.getEntryNames()) {
                    reposBuilder.add(RepoTip.TYPE.parseRequire(repoString));
                }
                ImmutableSet<RepoTip> repos = reposBuilder.build();

                QbtManifest.Builder newManifest = manifest.builder();
                for(RepoTip repo : repos) {
                    RepoManifest.Builder repoManifest = RepoManifest.TYPE.builder();

                    RepoTip inlinedRepoTip = RepoTip.TYPE.of(inlinedRepoName, repo.tip);

                    RepoManifest.Builder inlinedManifest = newManifest.get(inlinedRepoTip);
                    if(inlinedManifest == null) {
                        inlinedManifest = RepoManifest.TYPE.builder();
                        inlinedManifest = inlinedManifest.set(RepoManifest.VERSION, Optional.empty());
                    }

                    String inlinedRepoPrefix = combinePrefix(inlinedPrefix, repo.toString());

                    for(Map.Entry<String, PackageManifest.Builder> e : inlinedManifest.get(RepoManifest.PACKAGES).map.entries()) {
                        String prefix = e.getValue().get(PackageManifest.METADATA).get(PackageMetadata.PREFIX);
                        String subprefix;
                        if(prefix.equals(inlinedRepoPrefix)) {
                            subprefix = "";
                        }
                        else if(prefix.startsWith(inlinedRepoPrefix + "/")) {
                            subprefix = prefix.substring(inlinedRepoPrefix.length() + 1);
                        }
                        else {
                            continue;
                        }

                        inlinedManifest = inlinedManifest.transform(RepoManifest.PACKAGES, (rmp) -> rmp.without(e.getKey()));
                        PackageManifest.Builder packageManifest = e.getValue().transform(PackageManifest.METADATA, (md) -> md.set(PackageMetadata.PREFIX, subprefix));

                        repoManifest = repoManifest.transform(RepoManifest.PACKAGES, (rmp) -> rmp.with(e.getKey(), packageManifest));
                    }

                    VcsTreeDigest repoTree = inlinedTree.get(repo.toString()).leftOrNull().getDigest();
                    ImmutableList.Builder<VcsVersionDigest> satelliteParents = ImmutableList.builder();
                    for(VcsVersionDigest metaParent : cd.get(CommitData.PARENTS)) {
                        QbtManifest parentManifest = config.manifestParser.parse(ImmutableList.copyOf(metaRepository.showFile(metaParent, "qbt-manifest")));
                        RepoManifest parentRepoManifest = parentManifest.repos.get(repo);
                        if(parentRepoManifest == null) {
                            continue;
                        }
                        satelliteParents.add(parentRepoManifest.get(RepoManifest.VERSION).get());
                    }
                    CommitData.Builder satelliteCd = cd;
                    satelliteCd = satelliteCd.set(CommitData.TREE, repoTree);
                    satelliteCd = satelliteCd.set(CommitData.PARENTS, satelliteParents.build());
                    VcsVersionDigest repoVersion = HistoryRebuilder.cleanUpAndCommit(metaRepository, satelliteCd.build());
                    config.localPinsRepo.addPin(repo, metaRepository.getRoot(), repoVersion);

                    repoManifest = repoManifest.set(RepoManifest.VERSION, Optional.of(repoVersion));
                    newManifest = newManifest.with(repo, repoManifest);

                    newManifest = newManifest.with(inlinedRepoTip, inlinedManifest);
                }

                // remove remaining inlined repos, all must be empty
                for(Map.Entry<RepoTip, RepoManifest.Builder> e : newManifest.map.entries()) {
                    if(!e.getKey().name.equals(inlinedRepoName)) {
                        continue;
                    }
                    if(e.getValue().get(RepoManifest.PACKAGES).map.isEmpty()) {
                        newManifest = newManifest.without(e.getKey());
                        continue;
                    }
                    throw new IllegalArgumentException("Ended up with non-empty inlined repo at end of extract: " + e.getKey());
                }

                QbtManifest newManifestBuilt = newManifest.build();

                // check would-be inlined packages agree
                ImmutableSet<RepoTip> repos2 = ImmutableSet.copyOf(Options.repos.getRepos(config, newManifestBuilt, options));
                if(!repos.equals(repos2)) {
                    throw new IllegalArgumentException("Disagreement about what should have been inlined: " + repos + " versus " + repos2);
                }

                newTree = newTree.replace("qbt-manifest", Submanifest.linesToBytes(config.manifestParser.deparse(newManifestBuilt)));
                cd = cd.set(CommitData.TREE, newTree.getDigest());

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
                cd = naive.inline(cd);
                cd = addHeader(cd, base);
                return ComputationTree.constant(metaRepository.createCommit(cd.build()));
            }

            @Override
            protected ComputationTree<VcsVersionDigest> map(VcsVersionDigest commit, CommitData cd, ImmutableList<VcsVersionDigest> parents) {
                if(parseHeader(cd.get(CommitData.MESSAGE)) != null) {
                    throw new IllegalStateException("Commit already has header: " + commit);
                }

                CommitData.Builder inlined = naive.inline(cd.builder().set(CommitData.PARENTS, parents));
                CommitData.Builder naiveExtract = naive.extract(inlined.set(CommitData.PARENTS, cd.get(CommitData.PARENTS)));

                if(!cd.equals(naiveExtract.build())) {
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

    private static String combinePrefix(String a, String b) {
        if(a.isEmpty()) {
            return b;
        }
        if(b.isEmpty()) {
            return a;
        }
        return a + "/" + b;
    }
}
