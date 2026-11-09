import { readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

const root = fileURLToPath(new URL('../', import.meta.url));
let checked = 0;
function check(directory) {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      check(path);
    } else if (entry.name.endsWith('.js')) {
      const result = spawnSync(process.execPath, ['--check', path], { stdio: 'inherit' });
      if (result.error) throw result.error;
      if (result.status !== 0) process.exit(result.status ?? 1);
      checked++;
    }
  }
}
for (const directory of ['src', 'test', 'examples', 'scripts']) {
  check(join(root, directory));
}
console.log(`Syntax checked ${checked} JavaScript files.`);
