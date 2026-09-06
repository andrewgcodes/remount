import { describe, expect, it } from 'vitest';
import { parseCommand } from '../src/command';

describe('command input', () => {
  it('preserves shell-style quoted arguments without interpreting their contents', () => {
    expect(parseCommand(`printf "%s\\n" "hello world" ''`)).toEqual(['printf', '%s\\n', 'hello world', '']);
  });

  it('supports escaped separators and quotes', () => {
    expect(parseCommand(`printf hello\\ world "say \\"hello\\"" 'say "hello"'`)).toEqual([
      'printf',
      'hello world',
      'say "hello"',
      'say "hello"',
    ]);
  });

  it('rejects incomplete quoting instead of sending the wrong command', () => {
    expect(() => parseCommand(`printf "unfinished`)).toThrow('unterminated double quote');
    expect(() => parseCommand('printf trailing\\')).toThrow('incomplete escape');
  });
});
