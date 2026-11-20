import { execFile } from "node:child_process";
import { mkdtemp, mkdir, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { expect, test } from "vitest";

const exec = promisify(execFile);
const script = fileURLToPath(new URL("./upload-snapshot.sh", import.meta.url));

async function writeExec(path: string, body: string) {
  await writeFile(path, body, { mode: 0o755 });
}

test("upload step exits nonzero when all three S3 sync attempts fail", async () => {
  const dir = await mkdtemp(join(tmpdir(), "upload-snapshot-"));
  const bin = join(dir, "bin");
  await mkdir(bin);
  const awsCalls = join(dir, "aws.calls");

  await writeExec(
    join(bin, "aws"),
    `#!/bin/bash
echo "$@" >> "${awsCalls}"
exit 1
`,
  );
  await writeExec(join(bin, "which"), `#!/bin/bash\n[[ "$1" == aws ]] && exit 0\nexit 1\n`);
  await writeExec(join(bin, "sleep"), "#!/bin/bash\nexit 0\n");
  await writeExec(join(bin, "tar"), "#!/bin/bash\nexit 0\n");
  await writeExec(
    join(bin, "buildkite-agent"),
    `#!/bin/bash
if [[ "$1" == meta-data && "$2" == get ]]; then
  echo dummyvalue
  exit 0
fi
if [[ "$1" == artifact && "$2" == download ]]; then
  : > lucene-snapshot.tar.gz
  exit 0
fi
exit 0
`,
  );

  const result = await exec("bash", [script], {
    cwd: dir,
    env: {
      PATH: `${bin}:/usr/bin:/bin`,
      HOME: dir,
      LUCENE_BUILD_ID: "fixture-build",
      TARGET_BRANCH: "lucene_snapshot_11",
    },
  }).then(
    () => ({ status: 0, stderr: "" }),
    (error: NodeJS.ErrnoException & { status?: number; stderr?: string }) => ({
      status: error.status ?? 1,
      stderr: String(error.stderr ?? ""),
    }),
  );

  expect(result.status).toBe(1);
  expect(result.stderr).toMatch(/S3 upload failed after 3 attempts/);
  const calls = (await readFile(awsCalls, "utf8")).trim().split("\n");
  expect(calls).toHaveLength(3);
  expect(calls[0]).toMatch(/^s3 sync /);
});
