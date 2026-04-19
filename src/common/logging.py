import logging
import sys
from typing import Optional


def get_logger(name: str, *, level: int = logging.INFO, run_id: Optional[str] = None) -> logging.Logger:
    """
    Logger simples com saída em stdout.

    O `run_id` é embutido no prefixo para facilitar rastreio em logs agregados.
    """

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        run_prefix = f"[run_id={run_id}] " if run_id else ""
        formatter = logging.Formatter(f"%(asctime)sZ %(levelname)s %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # Adapter para prefixar mensagens com run_id (sem modificar o formatter global)
    if run_id:

        class _RunPrefixAdapter(logging.LoggerAdapter):
            def process(self, msg, kwargs):
                return f"[run_id={run_id}] {msg}", kwargs

        return _RunPrefixAdapter(logger, {})

    return logger

