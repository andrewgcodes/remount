export function parseCommand(input: string): string[] {
  const argv: string[] = [];
  let value = '';
  let started = false;
  let quote: "'" | '"' | undefined;

  const push = () => {
    if (!started) return;
    argv.push(value);
    value = '';
    started = false;
  };

  for (let i = 0; i < input.length; i += 1) {
    const char = input[i]!;
    if (quote === "'") {
      if (char === "'") quote = undefined;
      else value += char;
      continue;
    }
    if (quote === '"') {
      if (char === '"') {
        quote = undefined;
      } else if (char === '\\' && (input[i + 1] === '"' || input[i + 1] === '\\')) {
        value += input[i + 1];
        i += 1;
      } else {
        value += char;
      }
      continue;
    }
    if (/\s/.test(char)) {
      push();
    } else if (char === "'" || char === '"') {
      quote = char;
      started = true;
    } else if (char === '\\') {
      if (i + 1 === input.length) throw new Error('command ends with an incomplete escape');
      value += input[i + 1];
      started = true;
      i += 1;
    } else {
      value += char;
      started = true;
    }
  }
  if (quote) throw new Error(`command has an unterminated ${quote === "'" ? 'single' : 'double'} quote`);
  push();
  return argv;
}
