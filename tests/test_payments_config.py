"""Tests for payments configuration loading and validation."""

import json
import tempfile
from pathlib import Path

import pytest

from apex_core.config import load_payment_settings, PaymentSettings, PaymentMethod


class TestPaymentsConfig:
    """Test suite for payments configuration functionality."""

    def test_load_valid_payments_config(self):
        """Test loading a valid payments configuration file."""
        valid_config = {
            "payment_methods": [
                {
                    "name": "Wallet",
                    "instructions": "Use your existing wallet balance to complete this purchase instantly.",
                    "emoji": "💳",
                    "metadata": {"type": "internal"}
                },
                {
                    "name": "Binance",
                    "instructions": "Send the desired USD amount via Binance Pay and include your Discord username in the note field.",
                    "emoji": "🟡",
                    "metadata": {
                        "pay_id": "1234567890",
                        "reference": "Use your Discord username",
                        "url": "https://pay.binance.com/en"
                    }
                }
            ],
            "order_confirmation_template": "Order #{order_id} confirmed!\n\nService: {service_name} {variant_name}\nPrice: {price}\nETA: {eta}\n\nRefund Policy: 3 days from completion | 10% handling fee applied | 48hr ticket auto-close",
            "refund_policy": "3 days from completion | 10% handling fee applied | 48hr ticket auto-close"
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(valid_config, f)
            temp_path = Path(f.name)

        try:
            settings = load_payment_settings(temp_path)
            
            assert isinstance(settings, PaymentSettings)
            assert len(settings.payment_methods) == 2
            assert settings.payment_methods[0].name == "Wallet"
            assert settings.payment_methods[1].name == "Binance"
            assert settings.payment_methods[1].metadata["pay_id"] == "1234567890"
            assert "{order_id}" in settings.order_confirmation_template
            assert "{service_name}" in settings.order_confirmation_template
            assert "{variant_name}" in settings.order_confirmation_template
            assert "{price}" in settings.order_confirmation_template
            assert "{eta}" in settings.order_confirmation_template
            assert settings.refund_policy == "3 days from completion | 10% handling fee applied | 48hr ticket auto-close"
        finally:
            temp_path.unlink()

    def test_missing_file_raises_file_not_found(self):
        """Test that loading a non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="Payments configuration file not found"):
            load_payment_settings(Path("/non/existent/path.json"))

    def test_missing_required_fields(self):
        """Test validation of required fields."""
        # Test missing payment_methods
        config_missing_methods = {
            "order_confirmation_template": "Order #{order_id} confirmed!",
            "refund_policy": "3 days from completion"
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_missing_methods, f)
            temp_path = Path(f.name)

        try:
            with pytest.raises(ValueError, match="payment_methods field is required"):
                load_payment_settings(temp_path)
        finally:
            temp_path.unlink()

        # Test missing order_confirmation_template
        config_missing_template = {
            "payment_methods": [],
            "refund_policy": "3 days from completion"
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_missing_template, f)
            temp_path = Path(f.name)

        try:
            with pytest.raises(ValueError, match="order_confirmation_template field is required"):
                load_payment_settings(temp_path)
        finally:
            temp_path.unlink()

        # Test missing refund_policy
        config_missing_policy = {
            "payment_methods": [],
            "order_confirmation_template": "Order #{order_id} confirmed!"
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_missing_policy, f)
            temp_path = Path(f.name)

        try:
            with pytest.raises(ValueError, match="refund_policy field is required"):
                load_payment_settings(temp_path)
        finally:
            temp_path.unlink()

    def test_template_placeholder_validation(self):
        """Test validation of order confirmation template placeholders."""
        # Template missing {order_id}
        config_missing_order_id = {
            "payment_methods": [],
            "order_confirmation_template": "Service: {service_name} {variant_name}\nPrice: {price}\nETA: {eta}",
            "refund_policy": "3 days from completion"
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_missing_order_id, f)
            temp_path = Path(f.name)

        try:
            with pytest.raises(ValueError, match="Order confirmation template missing required placeholders.*order_id"):
                load_payment_settings(temp_path)
        finally:
            temp_path.unlink()

        # Template missing {service_name}
        config_missing_service = {
            "payment_methods": [],
            "order_confirmation_template": "Order #{order_id}\nVariant: {variant_name}\nPrice: {price}\nETA: {eta}",
            "refund_policy": "3 days from completion"
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_missing_service, f)
            temp_path = Path(f.name)

        try:
            with pytest.raises(ValueError, match="Order confirmation template missing required placeholders.*service_name"):
                load_payment_settings(temp_path)
        finally:
            temp_path.unlink()

    def test_payment_method_validation(self):
        """Test validation of payment method structure."""
        config_invalid_method = {
            "payment_methods": [
                {
                    # Missing required "name" field
                    "instructions": "Invalid method without name",
                    "emoji": "❌"
                }
            ],
            "order_confirmation_template": "Order #{order_id} confirmed!\n\nService: {service_name} {variant_name}\nPrice: {price}\nETA: {eta}",
            "refund_policy": "3 days from completion"
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_invalid_method, f)
            temp_path = Path(f.name)

        try:
            # This should raise a KeyError when trying to access the missing "name" field
            with pytest.raises(KeyError):
                load_payment_settings(temp_path)
        finally:
            temp_path.unlink()

    def test_empty_payment_methods_allowed(self):
        """Test that empty payment methods list is allowed."""
        config_empty_methods = {
            "payment_methods": [],
            "order_confirmation_template": "Order #{order_id} confirmed!\n\nService: {service_name} {variant_name}\nPrice: {price}\nETA: {eta}",
            "refund_policy": "3 days from completion | 10% handling fee applied | 48hr ticket auto-close"
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_empty_methods, f)
            temp_path = Path(f.name)

        try:
            settings = load_payment_settings(temp_path)
            assert isinstance(settings, PaymentSettings)
            assert len(settings.payment_methods) == 0
        finally:
            temp_path.unlink()

    def test_payment_method_optional_fields(self):
        """Test that optional fields in payment methods are handled correctly."""
        config_with_optional = {
            "payment_methods": [
                {
                    "name": "Minimal Method",
                    "instructions": "Just the basics"
                    # No emoji or metadata
                },
                {
                    "name": "Full Method",
                    "instructions": "Complete method with all fields",
                    "emoji": "🎯",
                    "metadata": {
                        "url": "https://example.com",
                        "extra": "additional info"
                    }
                }
            ],
            "order_confirmation_template": "Order #{order_id} confirmed!\n\nService: {service_name} {variant_name}\nPrice: {price}\nETA: {eta}",
            "refund_policy": "3 days from completion"
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_with_optional, f)
            temp_path = Path(f.name)

        try:
            settings = load_payment_settings(temp_path)
            assert len(settings.payment_methods) == 2
            
            # Check minimal method
            minimal = settings.payment_methods[0]
            assert minimal.name == "Minimal Method"
            assert minimal.emoji is None
            assert minimal.metadata == {}
            
            # Check full method
            full = settings.payment_methods[1]
            assert full.name == "Full Method"
            assert full.emoji == "🎯"
            assert full.metadata["url"] == "https://example.com"
            assert full.metadata["extra"] == "additional info"
        finally:
            temp_path.unlink()