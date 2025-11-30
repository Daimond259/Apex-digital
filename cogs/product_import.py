from __future__ import annotations

import csv
import io
import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from apex_core.utils import create_embed, format_usd

logger = logging.getLogger(__name__)


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

        # Validate file type
        if not csv_file.filename.lower().endswith('.csv'):
            await interaction.followup.send(
                "❌ Invalid file format. Please upload a CSV file.", ephemeral=True
            )
            return

        try:
            # Download file content
            file_content = await csv_file.read()
            
            # Try to decode as UTF-8, fallback to latin-1 if needed
            try:
                csv_text = file_content.decode('utf-8')
            except UnicodeDecodeError:
                csv_text = file_content.decode('latin-1')
            
            # Parse CSV
            csv_reader = csv.DictReader(io.StringIO(csv_text))
            
            # Validate required columns
            required_columns = [
                'Main_Category', 'Sub_Category', 'Service_Name', 
                'Variant_Name', 'Price_USD', 'Start_Time', 
                'Duration', 'Refill_Period', 'Additional_Info'
            ]
            
            if not csv_reader.fieldnames:
                await interaction.followup.send(
                    "❌ CSV file appears to be empty or invalid.", ephemeral=True
                )
                return
            
            missing_columns = [col for col in required_columns if col not in csv_reader.fieldnames]
            if missing_columns:
                await interaction.followup.send(
                    f"❌ Missing required columns: {', '.join(missing_columns)}\n\n"
                    f"Required columns: {', '.join(required_columns)}", ephemeral=True
                )
                return
            
            # Process products
            added_count = 0
            updated_count = 0
            skipped_count = 0
            active_product_ids = []
            errors = []
            
            for row_num, row in enumerate(csv_reader, start=2):  # Start at 2 to account for header
                try:
                    # Extract and validate data
                    main_category = row['Main_Category'].strip()
                    sub_category = row['Sub_Category'].strip()
                    service_name = row['Service_Name'].strip()
                    variant_name = row['Variant_Name'].strip()
                    
                    if not all([main_category, sub_category, service_name, variant_name]):
                        errors.append(f"Row {row_num}: Missing required field values")
                        skipped_count += 1
                        continue
                    
                    # Parse price
                    price_usd_str = row['Price_USD'].strip()
                    try:
                        price_usd = float(price_usd_str)
                        if price_usd < 0:
                            errors.append(f"Row {row_num}: Price cannot be negative")
                            skipped_count += 1
                            continue
                        price_cents = int(round(price_usd * 100))
                    except ValueError:
                        errors.append(f"Row {row_num}: Invalid price format '{price_usd_str}'")
                        skipped_count += 1
                        continue
                    
                    # Extract optional fields
                    start_time = row['Start_Time'].strip() or None
                    duration = row['Duration'].strip() or None
                    refill_period = row['Refill_Period'].strip() or None
                    additional_info = row['Additional_Info'].strip() or None
                    
                    # Check if product exists
                    existing_product = await self.bot.db.find_product_by_fields(
                        main_category=main_category,
                        sub_category=sub_category,
                        service_name=service_name,
                        variant_name=variant_name,
                    )
                    
                    if existing_product:
                        # Update existing product
                        await self.bot.db.update_product(
                            existing_product['id'],
                            price_cents=price_cents,
                            start_time=start_time,
                            duration=duration,
                            refill_period=refill_period,
                            additional_info=additional_info,
                            is_active=True,
                        )
                        updated_count += 1
                        active_product_ids.append(existing_product['id'])
                    else:
                        # Create new product
                        product_id = await self.bot.db.create_product(
                            main_category=main_category,
                            sub_category=sub_category,
                            service_name=service_name,
                            variant_name=variant_name,
                            price_cents=price_cents,
                            start_time=start_time,
                            duration=duration,
                            refill_period=refill_period,
                            additional_info=additional_info,
                        )
                        added_count += 1
                        active_product_ids.append(product_id)
                
                except Exception as e:
                    logger.error("Error processing row %d: %s", row_num, e)
                    errors.append(f"Row {row_num}: {str(e)}")
                    skipped_count += 1
                    continue
            
            # Soft delete products not in this import
            deactivated_count = 0
            if active_product_ids:
                deactivated_count = await self.bot.db.deactivate_all_products_except(active_product_ids)
            
            # Get total product count
            all_products = await self.bot.db.get_all_products(active_only=True)
            total_active = len(all_products)
            
            # Create result embed
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
                    name="⚠️ Skipped Rows",
                    value=f"{skipped_count} rows had errors and were skipped",
                    inline=False,
                )
            
            if errors and len(errors) <= 5:
                embed.add_field(
                    name="📋 Errors",
                    value="\n".join(errors[:5]),
                    inline=False,
                )
            elif errors:
                embed.add_field(
                    name="📋 Errors",
                    value=f"{len(errors)} errors occurred. First few:\n" + "\n".join(errors[:3]),
                    inline=False,
                )
            
            # Add template note if it exists
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
                pass  # Ignore template check errors
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
            # Log to order_logs channel for audit trail
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
                                f"**Skipped:** {skipped_count}\n"
                                f"**Total Active:** {total_active}"
                            ),
                            color=discord.Color.blue(),
                        )
                        if errors:
                            log_embed.add_field(
                                name="Errors Summary",
                                value=f"{len(errors)} errors occurred during import",
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