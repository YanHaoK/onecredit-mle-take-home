import os
import unittest

import numpy as np

from app.embeddings import Embedder


@unittest.skipUnless(os.environ.get("ONECREDIT_TEST_MODEL") == "1", "requires MiniLM model files")
class BilingualEmbeddings(unittest.TestCase):
    def test_rubber_glove_translation_outranks_rubberwood_furniture(self):
        encoder = Embedder()
        gloves_ms, gloves_en, furniture_ms = np.asarray([
            encoder.encode("Pengeluaran sarung tangan getah untuk eksport"),
            encoder.encode("Production of rubber gloves for export"),
            encoder.encode("Pembuatan perabot daripada kayu getah"),
        ])
        for vector in (gloves_ms, gloves_en, furniture_ms):
            self.assertEqual(vector.shape, (384,))
            self.assertAlmostEqual(np.linalg.norm(vector), 1.0, places=5)
        self.assertGreater(gloves_ms @ gloves_en, gloves_ms @ furniture_ms)
