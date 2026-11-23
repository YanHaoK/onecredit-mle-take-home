"""Encode business descriptions with the pinned multilingual MiniLM CPU model."""

import numpy as np
import onnxruntime as ort
from huggingface_hub import hf_hub_download
from tokenizers import Tokenizer

from app.config import MODEL_REPO, MODEL_REVISION

DIMENSION = 384


class Embedder:
    def __init__(self) -> None:
        tokenizer_file = hf_hub_download(
            MODEL_REPO, "tokenizer.json", revision=MODEL_REVISION
        )
        model_file = hf_hub_download(
            MODEL_REPO, "model_optimized.onnx", revision=MODEL_REVISION
        )
        self.tokenizer = Tokenizer.from_file(tokenizer_file)
        self.tokenizer.enable_truncation(max_length=128)
        options = ort.SessionOptions()
        options.intra_op_num_threads = 2
        self.session = ort.InferenceSession(
            model_file, sess_options=options, providers=["CPUExecutionProvider"]
        )
        self.input_names = [item.name for item in self.session.get_inputs()]

    def encode(self, text: str) -> list[float]:
        tokens = self.tokenizer.encode(text)
        inputs = {
            "input_ids": np.array([tokens.ids], dtype=np.int64),
            "attention_mask": np.array([tokens.attention_mask], dtype=np.int64),
            "token_type_ids": np.array([tokens.type_ids], dtype=np.int64),
        }
        hidden, = self.session.run(
            ["last_hidden_state"], {name: inputs[name] for name in self.input_names}
        )
        mask = inputs["attention_mask"][0].astype(bool)
        vector = hidden[0, mask].mean(axis=0)
        norm = np.linalg.norm(vector)
        if vector.shape != (DIMENSION,) or not np.isfinite(norm) or norm == 0:
            raise ValueError("invalid MiniLM embedding")
        return (vector / norm).tolist()
