#!/usr/bin/env python3
# Main.py — minimal ChatGPT CLI with token usage output

import os
import sys
import json
import argparse
from openai import OpenAI


def mask_key(k: str) -> str:
    if not k:
        return ""
    return k if len(k) <= 8 else f"{k[:4]}...{k[-4:]}"


def get_api_key() -> str:
    return os.environ.get("OPENAI_API_KEY", "")


def ask_chatgpt(prompt: str, model: str = "gpt-4o-mini") -> dict:
    api_key = get_api_key()
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY not found. Set it in your shell "
            '(e.g. PowerShell: $env:OPENAI_API_KEY="...") or add it to a .env file.'
        )

    client = OpenAI(api_key=api_key)

    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
    )

    text = resp.choices[0].message.content

    # usage is a pydantic model with these attributes
    u = resp.usage
    prompt_tokens = u.prompt_tokens
    completion_tokens = u.completion_tokens
    total_tokens = u.total_tokens

    return {
        "text": text,
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
        },
        "request_id": resp.id,
        "model": model,
        "_key_used": mask_key(api_key),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Tiny CLI to talk to ChatGPT via OpenAI API (with token usage)."
    )
    parser.add_argument("-t", "--text", help="One-shot prompt text.")
    parser.add_argument("-f", "--file", help="Read prompt from a UTF-8 text file.")
    parser.add_argument("-m", "--model", default="gpt-4o-mini", help="Model to use.")
    parser.add_argument("--json", action="store_true", help="Output JSON (for piping).")
    args = parser.parse_args()

    # Resolve the prompt source: --text, --file, or stdin
    prompt = None
    if args.text:
        prompt = args.text
    elif args.file:
        with open(args.file, "r", encoding="utf-8") as fh:
            prompt = fh.read()
    elif not sys.stdin.isatty():
        prompt = sys.stdin.read()

    if prompt is not None:
        # One-shot mode
        try:
            res = ask_chatgpt(prompt, args.model)
        except Exception as e:
            print(f"[error] {e}", file=sys.stderr)
            sys.exit(1)

        if args.json:
            print(json.dumps(res, ensure_ascii=False, indent=2))
        else:
            print(f"[using key] {res['_key_used']}")
            print(f"[model] {res['model']}")
            u = res["usage"]
            print(f"[tokens] prompt={u['prompt_tokens']}, completion={u['completion_tokens']}, total={u['total_tokens']}")
            print(f"[request_id] {res['request_id']}")
            print("\nChatGPT:\n" + res["text"])
        return

    # Interactive mode
    api_key = get_api_key()
    print(f"Model: {args.model}")
    print(f"[using key] {mask_key(api_key) if api_key else 'NOT SET'}")
    print('Type "exit" or "quit" to leave.\n')

    while True:
        try:
            user = input("You: ")
            if user.strip().lower() in {"exit", "quit"}:
                break

            res = ask_chatgpt(user, args.model)
            u = res["usage"]
            print(f"[tokens] prompt={u['prompt_tokens']}, completion={u['completion_tokens']}, total={u['total_tokens']}") #Will remove
            print(f"[request_id] {res['request_id']}") #Will remove
            print(f"ChatGPT: {res['text']}\n")

        except KeyboardInterrupt:
            print()
            break
        except Exception as e:
            print(f"[error] {e}", file=sys.stderr)
            break


if __name__ == "__main__":
    main()
