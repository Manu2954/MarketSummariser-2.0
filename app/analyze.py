from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import pandas as pd
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze a CSV with Qwen using instructions.")
    parser.add_argument("--csv", required=True, help="Path to the CSV file.")
    parser.add_argument("--instructions", default="data/instructions.txt", help="Path to the instructions file.")
    parser.add_argument("--model", default="Qwen/Qwen2.5-32B-Instruct", help="Hugging Face model id or local path.")
    parser.add_argument("--max-rows", type=int, default=200, help="Maximum number of rows to include from the CSV (tail).")
    parser.add_argument("--columns", nargs="*", help="Optional subset of columns to include.")
    parser.add_argument("--temperature", type=float, default=0.2)
    parser.add_argument("--top-p", type=float, default=0.9)
    parser.add_argument("--max-new-tokens", type=int, default=512)
    parser.add_argument("--attn-impl", choices=["eager", "flash_attention_2"], help="Attention implementation override.")
    return parser.parse_args()


def load_instructions(path: Path) -> str:
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def load_csv(path: Path, max_rows: int, columns: Optional[list[str]]) -> pd.DataFrame:
    df = pd.read_csv(path)
    if columns:
        existing = [c for c in columns if c in df.columns]
        df = df[existing]
    if max_rows and len(df) > max_rows:
        df = df.tail(max_rows)
    return df.reset_index(drop=True)


def build_prompt(instructions: str, df: pd.DataFrame, csv_path: Path) -> str:
    preview = df.to_markdown(index=False)
    meta_lines = [
        f"file: {csv_path}",
        f"rows_in_sample: {len(df)}",
        f"columns: {', '.join(df.columns)}",
    ]
    meta = "\n".join(meta_lines)
    return (
        f"You are an analyst.\n"
        f"Instructions:\n{instructions}\n\n"
        f"Dataset metadata:\n{meta}\n\n"
        f"Sample data (tabular, markdown):\n{preview}\n"
    )


def generate(
    prompt_text: str,
    model_id: str,
    temperature: float,
    top_p: float,
    max_new_tokens: int,
    attn_impl: Optional[str] = None,
) -> str:
    tokenizer = AutoTokenizer.from_pretrained(model_id, trust_remote_code=True)
    model_kwargs = {
        "device_map": "auto",
        "torch_dtype": "auto",
        "trust_remote_code": True,
    }
    if attn_impl:
        model_kwargs["attn_implementation"] = attn_impl
    model = AutoModelForCausalLM.from_pretrained(model_id, **model_kwargs)
    messages = [
        {"role": "system", "content": "You are a careful financial data analyst."},
        {"role": "user", "content": prompt_text},
    ]
    chat_prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    inputs = tokenizer(chat_prompt, return_tensors="pt").to(model.device)
    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=max_new_tokens,
            do_sample=temperature > 0,
            temperature=temperature,
            top_p=top_p,
            eos_token_id=tokenizer.eos_token_id,
        )
    generated = outputs[0][inputs["input_ids"].shape[1] :]
    return tokenizer.decode(generated, skip_special_tokens=True)


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv)
    instr_path = Path(args.instructions)

    if not csv_path.exists():
        print(f"CSV not found: {csv_path}")
        return 1
    if not instr_path.exists():
        print(f"Instructions file not found: {instr_path}")
        return 1

    instructions = load_instructions(instr_path)
    df = load_csv(csv_path, args.max_rows, args.columns)
    if df.empty:
        print("No data to analyze after filtering.")
        return 1

    prompt_text = build_prompt(instructions, df, csv_path)
    print("Running model... (this may take time for large models)")
    response = generate(
        prompt_text,
        model_id=args.model,
        temperature=args.temperature,
        top_p=args.top_p,
        max_new_tokens=args.max_new_tokens,
        attn_impl=args.attn_impl,
    )
    print("\n=== Model Output ===\n")
    print(response.strip())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
