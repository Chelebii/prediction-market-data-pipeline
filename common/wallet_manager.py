"""
wallet_manager.py -- Wallet & Credential Yonetimi
=================================================
Polygon wallet bakiye sorgulama ve credential yuklemesi.
Private key ASLA logllanmaz veya diske yazilmaz.
"""

import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple

import requests
from eth_account import Account

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

logger = logging.getLogger("wallet_manager")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("[%(asctime)s] [WALLET] %(message)s", "%Y-%m-%d %H:%M:%S"))
    logger.addHandler(_h)

# Polygon USDC contracts
USDC_BRIDGED = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # PoS Bridge USDC (6 decimals)
USDC_NATIVE = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"   # Native USDC (6 decimals)

# Polygon RPC endpoints (fallback listesi)
RPC_ENDPOINTS = [
    "https://polygon-rpc.com",
    "https://rpc-mainnet.matic.quiknode.pro",
    "https://polygon.llamarpc.com",
]

# ERC-20 balanceOf ABI (minimal)
BALANCE_OF_SELECTOR = "0x70a08231"  # balanceOf(address)

# Credential env variable isimleri
CREDENTIAL_KEYS = {
    "private_key": "POLY_PRIVATE_KEY",
    "api_key": "POLY_API_KEY",
    "api_secret": "POLY_API_SECRET",
    "api_passphrase": "POLY_API_PASSPHRASE",
    "funder_address": "POLY_FUNDER_ADDRESS",
    "signature_type": "POLY_SIGNATURE_TYPE",
}


def _mask_address(address: str) -> str:
    """Wallet adresini maskeler: 0x1234...abcd"""
    if not address or len(address) < 10:
        return address or "N/A"
    return f"{address[:6]}...{address[-4:]}"


def _mask_key(key: str) -> str:
    """Private key'i maskeler: sadece uzunluk gosterir."""
    if not key:
        return "N/A"
    return f"***({len(key)} chars)"


class WalletManager:
    """
    Polygon wallet yonetimi.
    Private key env variable'dan okunur, ASLA diske yazilmaz.
    """

    def __init__(self, private_key: Optional[str] = None, private_key_env: str = "POLY_PRIVATE_KEY"):
        self._private_key = private_key or os.getenv(private_key_env, "").strip()
        self._account = None
        self._address = None

        if self._private_key:
            try:
                # 0x prefix yoksa ekle
                pk = self._private_key if self._private_key.startswith("0x") else f"0x{self._private_key}"
                self._account = Account.from_key(pk)
                self._address = self._account.address
                logger.info("Wallet yuklendi: %s", _mask_address(self._address))
            except Exception as e:
                logger.error("Gecersiz private key: %s", e)
                self._account = None
                self._address = None

    # --------------------------------------------------------- public methods
    def get_address(self) -> Optional[str]:
        """Wallet adresini doner."""
        return self._address

    def get_masked_address(self) -> str:
        """Maskelenmis wallet adresini doner."""
        return _mask_address(self._address or "")

    def is_valid(self) -> bool:
        """Private key gecerli ve wallet adresi turetilebildi mi?"""
        return self._account is not None and self._address is not None

    def get_usdc_balance(self, contract: str = USDC_BRIDGED) -> Optional[float]:
        """
        Polygon USDC bakiyesini sorgular.
        Basit eth_call kullanir -- web3 bagimliligi gerekmez.
        Returns: USDC miktari (float) veya None (hata durumunda)
        """
        if not self._address:
            logger.error("Wallet adresi yok -- bakiye sorgulanamaz.")
            return None

        # balanceOf(address) calldata
        padded_address = self._address[2:].lower().zfill(64)
        calldata = f"{BALANCE_OF_SELECTOR}{padded_address}"

        for rpc_url in RPC_ENDPOINTS:
            try:
                payload = {
                    "jsonrpc": "2.0",
                    "method": "eth_call",
                    "params": [
                        {"to": contract, "data": calldata},
                        "latest",
                    ],
                    "id": 1,
                }
                resp = requests.post(rpc_url, json=payload, timeout=10)
                if resp.status_code != 200:
                    continue

                result = resp.json().get("result", "0x0")
                balance_raw = int(result, 16)
                balance_usdc = balance_raw / 1_000_000  # 6 decimals
                logger.info(
                    "USDC bakiye (%s): $%.2f [%s]",
                    _mask_address(self._address),
                    balance_usdc,
                    "bridged" if contract == USDC_BRIDGED else "native",
                )
                return balance_usdc

            except Exception as e:
                logger.warning("RPC hatasi (%s): %s", rpc_url, e)
                continue

        logger.error("Tum RPC endpoint'leri basarisiz -- USDC bakiyesi alinamadi.")
        return None

    def get_total_usdc_balance(self) -> Optional[float]:
        """Hem bridged hem native USDC bakiyesinin toplamini doner."""
        bridged = self.get_usdc_balance(USDC_BRIDGED)
        native = self.get_usdc_balance(USDC_NATIVE)

        if bridged is None and native is None:
            return None
        return (bridged or 0.0) + (native or 0.0)

    def get_matic_balance(self) -> Optional[float]:
        """Polygon MATIC (gas) bakiyesini sorgular."""
        if not self._address:
            return None

        for rpc_url in RPC_ENDPOINTS:
            try:
                payload = {
                    "jsonrpc": "2.0",
                    "method": "eth_getBalance",
                    "params": [self._address, "latest"],
                    "id": 1,
                }
                resp = requests.post(rpc_url, json=payload, timeout=10)
                if resp.status_code != 200:
                    continue

                result = resp.json().get("result", "0x0")
                balance_wei = int(result, 16)
                balance_matic = balance_wei / 1e18
                return balance_matic

            except Exception:
                continue

        return None

    def validate_credentials(self) -> Tuple[bool, str]:
        """
        Credential'larin gecerliligini kontrol eder.
        Returns: (gecerli_mi, hata_mesaji)
        """
        issues = []

        if not self._private_key:
            issues.append("POLY_PRIVATE_KEY eksik")
        elif not self.is_valid():
            issues.append("POLY_PRIVATE_KEY gecersiz format")

        if not os.getenv("POLY_API_KEY", "").strip():
            issues.append("POLY_API_KEY eksik (ilk calistirmada otomatik turetilecek)")

        if issues:
            return False, "; ".join(issues)

        return True, "Tum credential'lar gecerli."


def load_credentials_from_env() -> Dict[str, str]:
    """
    .env'den tum POLY_* degiskenlerini okur ve dict doner.
    Eksik olanlari loglar (ama hata vermez).
    Private key ASLA loglanmaz.
    """
    creds = {}
    missing = []

    for key_name, env_name in CREDENTIAL_KEYS.items():
        val = os.getenv(env_name, "").strip()
        if val:
            creds[key_name] = val
        else:
            missing.append(env_name)

    if missing:
        logger.warning("Eksik credential(lar): %s", ", ".join(missing))
    else:
        logger.info("Tum credential'lar yuklendi.")

    # Guvenlik: private key'i loglamiyoruz
    if "private_key" in creds:
        logger.info("Private key: %s", _mask_key(creds["private_key"]))

    return creds
