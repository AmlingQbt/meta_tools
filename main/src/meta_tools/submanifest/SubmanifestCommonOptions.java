package meta_tools.submanifest;

import com.google.common.collect.ImmutableList;
import misc1.commons.Maybe;
import misc1.commons.options.NamedStringListArgumentOptionsFragment;
import misc1.commons.options.NamedStringSingletonArgumentOptionsFragment;
import misc1.commons.options.OptionsFragment;
import qbt.QbtCommandOptions;
import qbt.options.ConfigOptionsDelegate;
import qbt.options.ParallelismOptionsDelegate;

public interface SubmanifestCommonOptions extends QbtCommandOptions {
    public static final ConfigOptionsDelegate<SubmanifestCommonOptions> config = new ConfigOptionsDelegate<SubmanifestCommonOptions>();
    public static final OptionsFragment<SubmanifestCommonOptions, ?, String> metaVcs = new NamedStringSingletonArgumentOptionsFragment<SubmanifestCommonOptions>(ImmutableList.of("--metaVcs"), Maybe.of("git"), "VCS for meta");
    public static final ParallelismOptionsDelegate<SubmanifestCommonOptions> parallelism = new ParallelismOptionsDelegate<SubmanifestCommonOptions>();
    public static final OptionsFragment<SubmanifestCommonOptions, ?, String> importedFile = new NamedStringSingletonArgumentOptionsFragment<SubmanifestCommonOptions>(ImmutableList.of("--importedFile"), Maybe.of("imported-repos"), "File in root of tree to use to track imported repos.");
    public static final OptionsFragment<SubmanifestCommonOptions, ?, ImmutableList<String>> base = new NamedStringListArgumentOptionsFragment<SubmanifestCommonOptions>(ImmutableList.of("--base"), "Treat this commit, and any of its ancestors as bases.");
}
