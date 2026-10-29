import { execFile } from 'child_process'
import { promisify } from 'util'

const execFileAsync = promisify(execFile)

async function git(cwd: string, ...args: string[]): Promise<string> {
  const { stdout } = await execFileAsync('git', args, { cwd, timeout: 15000, maxBuffer: 16 * 1024 * 1024, env: { ...process.env, GIT_OPTIONAL_LOCKS: '0' } })
  return stdout
}

export class GitAdapter {
  async getCurrentBranch(cwd: string): Promise<string> {
    return (await git(cwd, 'rev-parse', '--abbrev-ref', 'HEAD')).trim()
  }

  async hashObject(cwd: string, filePath: string): Promise<string> {
    return (await git(cwd, 'hash-object', filePath)).trim()
  }

  async listTrackedFiles(cwd: string): Promise<string[] | null> {
    try {
      const output = await git(cwd, 'ls-files', '-z', '--cached', '--others', '--exclude-standard')
      return output ? output.split('\0').filter(Boolean) : []
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code
      if (code === 'ENOENT' || String(code) === '128') return null
      throw error
    }
  }

  async diff(cwd: string, ...paths: string[]): Promise<string> {
    try {
      return await git(cwd, 'diff', '--', ...paths)
    } catch {
      return ''
    }
  }
}
