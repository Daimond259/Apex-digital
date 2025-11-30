from __future__ import annotations

import asyncio
import csv
import io
import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from apex_core.utils import create_embed, format_usd

logger = logging.getLogger(__name__)

MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024


def _parse_and_validate_csv(file_content: bytes) -> dict:
    """Parse and validate CSV file content. Runs in a thread executor.
    
    Returns:
        Dict with 'success' bool and either 'rows' or 'error' key.
        'rows' contains validated row data ready for DB lookups.
    """
    try:
        try:
            csv_text = file_content.decode('utf-8')
        except UnicodeDecodeError:
            csv_text = file_content.decode('latin-1')
        
        csv_reader = csv.DictReader(io.StringIO(csv_text))
        
        required_columns = [
            'Main_Category', 'Sub_Category', 'Service_Name', 
            'Variant_Name', 'Price_USD', 'Start_Time', 
            'Duration', 'Refill_Period', 'Additional_Info'
        ]
        
        if not csv_reader.fieldnames:
            return {'success': False, 'error': 'CSV file appears to be empty or invalid.'}
        
        missing_columns = [col for col in required_columns if col not in csv_reader.fieldnames]
        if missing_columns:
            return {
                'success': False,
                'error': f"Missing required columns: {', '.join(missing_columns)}"
            }
        
        validated_rows = []
        errors = []
        
        for row_num, row in enumerate(csv_reader, start=2):
            try:
                main_category = row['Main_Category'].strip()
                sub_category = row['Sub_Category'].strip()
                service_name = row['Service_Name'].strip()
                variant_name = row['Variant_Name'].strip()
                
                if not all([main_category, sub_category, service_name, variant_name]):
                    errors.append(f"Row {row_num}: Missing required field values")
                    continue
                
                price_usd_str = row['Price_USD'].strip()
                try:
                    price_usd = float(price_usd_str)
                    if price_usd < 0:
                        errors.append(f"Row {row_num}: Price cannot be negative")
                        continue
                    price_cents = int(round(price_usd * 100))
                except ValueError:
                    errors.append(f"Row {row_num}: Invalid price format '{price_usd_str}'")
                    continue
                
                start_time = row['Start_Time'].strip() or None
                duration = row['Duration'].strip() or None
                refill_period = row['Refill_Period'].strip() or None
                additional_info = row['Additional_Info'].strip() or None
                
                validated_rows.append({
                    'main_category': main_category,
                    'sub_category': sub_category,
                    'service_name': service_name,
                    'variant_name': variant_name,
                    'price_cents': price_cents,
                    'start_time': start_time,
                    'duration': duration,
                    'refill_period': refill_period,
                    'additional_info': additional_info,
                })
            
            except Exception as e:
                logger.error("Error processing row %d: %s", row_num, e)
                errors.append(f"Row {row_num}: {str(e)}")
        
        return {
            'success': True,
            'rows': validated_rows,
            'errors': errors,
        }
    
    except Exception as e:
        logger.error("CSV parsing failed: %s", e)
        return {'success': False, 'error': f"Failed to parse CSV: {str(e)}"}


class ProductImportCog(commands.Cog):
    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot

    def _is_admin(self, member: discord.Member | None) -> bool:
        if member is None:
            return False
        admin_role_id = self.bot.config.role_ids.admin
        return any(role.id == admin_role_id for role in getattr(member, "roles", []))

    def _resolve_member(self, interaction: discord.Interaction) -> discord.Member | None:
        if isinstance(interaction.user, discord.Member):
            return interaction.user
        if interaction.guild:
            return interaction.guild.get_member(interaction.user.id)
        return None

    @app_commands.command(
        name="import_products",
        description="Import products from a CSV file (admin only)",
    )
    @app_commands.describe(
        csv_file="CSV file with product data",
    )
    async def import_products(
        self,
        interaction: discord.Interaction,
        csv_file: discord.Attachment,
    ) -> None:
        """Import products from a CSV file."""
        member = self._resolve_member(interaction)
        if not self._is_admin(member):
            await interaction.response.send_message(
                "You do not have permission to use this command.", ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        if not csv_file.filename.lower().endswith('.csv'):
            await interaction.followup.send(
                "❌ Invalid file format. Please upload a CSV file.", ephemeral=True
            )
            return

        try:
            file_content = await csv_file.read()
            
            if len(file_content) > MAX_FILE_SIZE_BYTES:
                file_size_mb = len(file_content) / (1024 * 1024)
                max_size_mb = MAX_FILE_SIZE_BYTES / (1024 * 1024)
                logger.warning(
                    "CSV import rejected - file too large: %.2f MB (max: %.2f MB) from user %s",
                    file_size_mb, max_size_mb, interaction.user.id
                )
                await interaction.followup.send(
                    f"❌ File too large: {file_size_mb:.2f} MB. Maximum allowed: {max_size_mb:.0f} MB",
                    ephemeral=True
                )
                return
            
            parse_result = await asyncio.to_thread(_parse_and_validate_csv, file_content)
            
            if not parse_result['success']:
                error_msg = parse_result['error']
                logger.warning("CSV validation failed: %s", error_msg)
                await interaction.followup.send(
                    f"❌ {error_msg}", ephemeral=True
                )
                return
            
            validated_rows = parse_result['rows']
            validation_errors = parse_result['errors']
            
            if not validated_rows and not validation_errors:
                await interaction.followup.send(
                    "❌ CSV file is empty or contains no valid rows.", ephemeral=True
                )
                return
            
            products_to_add = []
            products_to_update = []
            active_product_ids = []
            skipped_count = len(validation_errors)
            
            for row in validated_rows:
                existing_product = await self.bot.db.find_product_by_fields(
                    main_category=row['main_category'],
                    sub_category=row['sub_category'],
                    service_name=row['service_name'],
                    variant_name=row['variant_name'],
                )
                
                if existing_product:
                    products_to_update.append({
                        'id': existing_product['id'],
                        'price_cents': row['price_cents'],
                        'start_time': row['start_time'],
                        'duration': row['duration'],
                        'refill_period': row['refill_period'],
                        'additional_info': row['additional_info'],
                        'is_active': 1,
                    })
                    active_product_ids.append(existing_product['id'])
                else:
                    products_to_add.append(row)
            
            added_count, updated_count, deactivated_count = await self.bot.db.bulk_upsert_products(
                products_to_add, products_to_update, active_product_ids
            )
            
            all_products = await self.bot.db.get_all_products(active_only=True)
            total_active = len(all_products)
            
            embed = create_embed(
                title="✅ Import Successful",
                description=(
                    f"**Added:** {added_count} products\n"
                    f"**Updated:** {updated_count} products\n"
                    f"**Deactivated:** {deactivated_count} products\n"
                    f"**Total Active Products:** {total_active}"
                ),
                color=discord.Color.green(),
            )
            
            if skipped_count > 0:
                embed.add_field(
                    name="⚠️ Validation Errors",
                    value=f"{skipped_count} rows had validation errors",
                    inline=False,
                )
            
            if validation_errors and len(validation_errors) <= 5:
                embed.add_field(
                    name="📋 Error Details",
                    value="\n".join(validation_errors[:5]),
                    inline=False,
                )
            elif validation_errors:
                embed.add_field(
                    name="📋 Error Details",
                    value=f"{len(validation_errors)} errors occurred. First few:\n" + "\n".join(validation_errors[:3]),
                    inline=False,
                )
            
            try:
                import os
                template_path = "/home/engine/project/templates/products_template.xlsx"
                if os.path.exists(template_path):
                    embed.add_field(
                        name="📋 Template",
                        value="Download template: `/templates/products_template.xlsx`",
                        inline=False,
                    )
            except Exception:
                pass
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
            if interaction.guild:
                log_channel_id = self.bot.config.logging_channels.order_logs
                if log_channel_id:
                    log_channel = interaction.guild.get_channel(log_channel_id)
                    if isinstance(log_channel, discord.TextChannel):
                        log_embed = create_embed(
                            title="📦 Products Import Completed",
                            description=(
                                f"**Admin:** {interaction.user.mention} ({interaction.user.id})\n"
                                f"**File:** {csv_file.filename}\n"
                                f"**Added:** {added_count}\n"
                                f"**Updated:** {updated_count}\n"
                                f"**Deactivated:** {deactivated_count}\n"
                                f"**Validation Errors:** {skipped_count}\n"
                                f"**Total Active:** {total_active}"
                            ),
                            color=discord.Color.blue(),
                        )
                        if validation_errors:
                            log_embed.add_field(
                                name="Errors Summary",
                                value=f"{len(validation_errors)} validation errors during import",
                                inline=False,
                            )
                        
                        try:
                            await log_channel.send(embed=log_embed)
                        except discord.HTTPException as e:
                            logger.error("Failed to log import to channel %s: %s", log_channel_id, e)
        
        except Exception as e:
            logger.error("Import failed for user %s: %s", interaction.user.id, e)
            await interaction.followup.send(
                f"❌ Import failed: {str(e)}", ephemeral=True
            )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(ProductImportCog(bot))