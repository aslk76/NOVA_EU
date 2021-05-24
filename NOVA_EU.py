#!/usr/bin/env python3
# coding=utf-8
import os
import traceback
import logging
from datetime import datetime, timedelta, timezone
import discord
from discord.ext import commands, tasks
from discord.utils import get
import aiomysql
import aiohttp
import asyncio
import socket
import collections
import requests
import json
import re
from dotenv import load_dotenv
import gspread_asyncio
from google.oauth2.service_account import Credentials
from string import ascii_lowercase


from constants import *
from functions import *

def get_creds():
    creds = Credentials.from_service_account_file("/NOVA/AutoBot/novabot-256801-85f98aa21edc.json")
    scoped = creds.with_scopes([
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    return scoped


agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)


running = False

boosters = []

react_users = []

troll_target = 0
troll_target1 = 0
troll_target2 = 0
troll_target3 = 0



load_dotenv()
token = os.getenv('DISCORD_TOKEN')
GUILD_ID = os.getenv('NOVA_ID')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
OPS_DB = os.getenv('OPS_DB')
MPLUS_DB =  os.getenv("MPLUS_DB")


intents = discord.Intents().all()

class EU_Bot(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self.mplus_pool = None
        self.ops_pool = None
        self._resolver = aiohttp.AsyncResolver()

        # Use AF_INET as its socket family to prevent HTTPS related problems both locally
        # and in production.
        self._connector = aiohttp.TCPConnector(
            resolver=self._resolver,
            family=socket.AF_INET,
        )

        self.http.connector = self._connector
        self.http_session = aiohttp.ClientSession(connector=self._connector)


    async def logout(self):
        """|coro|
        Logs out of Discord and closes all connections.
        """
        try:
            if self.mplus_pool:
                self.mplus_pool.close()
                await self.mplus_pool.wait_closed()
            if self.ops_pool:
                self.ops_pool.close()
                await self.ops_pool.wait_closed()
        finally:
            await super().logout()


bot= EU_Bot(command_prefix=commands.when_mentioned_or('!'), case_insensitive=True, intents=intents)

logger = logging.getLogger('discord')
logger.setLevel(logging.INFO)
# handler = logging.FileHandler(filename='/NOVA/NOVA_EU/NOVA_EU.log', encoding='utf-8', mode='a')
handler = logging.FileHandler(filename='NOVA_EU.log', encoding='utf-8', mode='a')
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s'))
logger.addHandler(handler)

class rio_conf:
    RAIDERIO_LINK = r"https:\/\/raider\.io\/characters\/eu\/(.+)\/([^?.]+)"
    base: str = "https://raider.io"
    role_threshhold: int = 1300

@bot.event
async def on_error(event, *args, **kwargs):
    logger.error(f"========On {event} error START=======")
    s = traceback.format_exc()
    content = f'Ignoring exception in {event}\n{s}'
    logger.error(content)
    logger.error(f"========On {event} error END=========")
    guild = bot.get_guild(GUILD_ID)
    bot_log_channel = get(guild.text_channels, name='bot-logs')
    embed_bot_log = discord.Embed(
        title=f"{bot.user.name} Error Log.", 
        description=event, 
        color=discord.Color.blue())
    embed_bot_log.set_footer(text=datetime.now(timezone.utc).replace(microsecond=0))
    await bot_log_channel.send(embed=embed_bot_log)


@bot.event
async def on_command_error(ctx, error):
    if (isinstance(error, commands.MissingRole) or 
        isinstance(error, commands.MissingAnyRole)):
        em = discord.Embed(title="❌ Missing permissions",
                           description="You don't have permission to use this command",
                           color=discord.Color.red())
        await ctx.send(embed=em, delete_after=10)
    elif isinstance(error, commands.CommandNotFound):
        em = discord.Embed(title="❌ No Such Command",
                           description="",
                           color=discord.Color.red())
        await ctx.send(embed=em, delete_after=5)
    elif isinstance(error, commands.BadArgument):
        em = discord.Embed(title="❌ Bad arguments",
                           description=error,
                           color=discord.Color.red())
        await ctx.send(embed=em, delete_after=5)
    elif isinstance(error, commands.MissingRequiredArgument):
        em = discord.Embed(title="❌ Missing arguments",
                           description=error,
                           color=discord.Color.red())
        await ctx.send(embed=em, delete_after=10)
    elif isinstance(error, commands.CommandOnCooldown):
        await ctx.message.delete()
        em = discord.Embed(title="❌ On Cooldown",
                           description="{ctx.command.name} is on cooldown, please try again in {:.2f}s".format(
                               error.retry_after),
                           color=discord.Color.red())
        await ctx.send(embed=em, delete_after=5)
    logger.error(f"========on {ctx.command.name} START=======")
    logger.error(f"traceback: {traceback.format_exc()}")
    logger.error(f"error: {error}")
    logger.error(f"========on {ctx.command.name} END=========")
    bot_log_channel = get(ctx.guild.text_channels, name='bot-logs')
    embed_bot_log = discord.Embed(
        title=f"{ctx.bot.user.name} Error Log.",
        description=f"on {ctx.command.name}",
        color=discord.Color.blue())
    embed_bot_log.set_footer(text=datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None))
    await bot_log_channel.send(embed=embed_bot_log)


@bot.event
async def on_ready():
    global boosters, running
    if running is False:
        logger.info(f'{bot.user.name} {discord.__version__} has connected to Discord!')
        guild = bot.get_guild(GUILD_ID)
        bot_log_channel = (get(guild.text_channels, id=817552283209433098) or 
                            get(guild.text_channels, name='bot-logs'))
        embed_bot_log = discord.Embed(
            title="Info Log.", 
            description=
                f'{bot.user.name} {discord.__version__} has connected to Discord!',
            color=0x5d4991)
        embed_bot_log.set_footer(text=datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None))
        await bot_log_channel.send(embed=embed_bot_log)
        async with bot.ops_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = "SELECT * FROM cross_faction_boosters"
                await cursor.execute(query)
                boosters = await cursor.fetchall()
        SuspensionCheck_loop.start()
        running = True


@bot.command(aliases=['ADA', 'AddCrossFaction'])
@commands.has_any_role('Bot Whisperer', 'Management')
async def AddDoubleAgent(ctx, discord_id :int, alliance_name, horde_name):
    """To manually add cross faction booster
    example: !AddDoubleAgent 163324686086832129 "Sanfura-Ravencrest [A]" "Sanfura-TarrenMill [H]"
    """
    global boosters
    async with ctx.bot.ops_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            query = """
                INSERT INTO cross_faction_boosters 
                    (discord_id, alliance_name, horde_name) 
                    VALUES (%s, %s, %s)
            """
            val = (discord_id, alliance_name, horde_name)
            await cursor.execute(query, val)
            await ctx.message.delete()
            await cursor.execute("SELECT * FROM cross_faction_boosters")
            boosters = await cursor.fetchall()


@tasks.loop(minutes=10)
async def SuspensionCheck_loop():
    guild = bot.get_guild(GUILD_ID)
    suspensionA_channel = get(guild.text_channels, name='suspension')
    suspensionH_channel = get(guild.text_channels, name='suspension-status')
    HighKeyBoosterA_role = get(guild.roles, name='High Key Booster [A]')
    MBoosterA_role = get(guild.roles, name='M+ Booster [A]')
    HighKeyBoosterH_role = get(guild.roles, name='High Key Booster [H]')
    MBoosterH_role = get(guild.roles, name='M+ Booster [H]')
    SuspendedA_role = get(guild.roles, name='Suspended')
    SuspendedH_role = get(guild.roles, name='Suspended {H}')
    async with bot.ops_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT * FROM suspension ORDER BY duration desc")
            myresult = await cursor.fetchall()        
            for x in myresult:
                if (datetime.strptime(x[4], '%Y-%m-%d %H:%M:%S.%f') < 
                    datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)):
                    member_fromDB = guild.get_member(x[0])
                    if x[1] == "High Key Booster [A]":
                        await member_fromDB.add_roles(HighKeyBoosterA_role, MBoosterA_role)
                        await member_fromDB.remove_roles(SuspendedA_role)
                        await suspensionA_channel.send(f"{member_fromDB.mention} Suspension has been lifted.")
                    elif x[1] == "High Key Booster [H]":
                        await member_fromDB.add_roles(HighKeyBoosterH_role, MBoosterH_role)
                        await member_fromDB.remove_roles(SuspendedH_role)
                        await suspensionH_channel.send(f"{member_fromDB.mention} Suspension has been lifted.")
                    elif x[1] == "--" and x[2] == "M+ Booster [A]":
                        await member_fromDB.add_roles(MBoosterA_role)
                        await member_fromDB.remove_roles(SuspendedA_role)
                        await suspensionA_channel.send(f"{member_fromDB.mention} Suspension has been lifted.")
                    elif x[1] == "--" and x[2] == "M+ Booster [H]":
                        await member_fromDB.add_roles(MBoosterH_role)
                        await member_fromDB.remove_roles(SuspendedH_role)
                        await suspensionH_channel.send(f"{member_fromDB.mention} Suspension has been lifted.")
                    query = "DELETE FROM suspension WHERE username = %s"
                    val = (x[0],)
                    await cursor.execute(query, val)


@bot.command()
@commands.has_any_role('Moderator', 'NOVA')
async def checkRole(ctx):
    """To manually assign PickYourRegion for those who are eligible
    """
    alliance_role = get(ctx.guild.roles, name="Alliance")
    horde_role = get(ctx.guild.roles, name="Horde")
    client_role = get(ctx.guild.roles, name="Client")
    clientNA_role = get(ctx.guild.roles, name="Client NA")
    pickyourregion_role = get(ctx.guild.roles, name="PickYourRegion")
    nova_role = get(ctx.guild.roles, name="NOVA")
    moderator_role = get(ctx.guild.roles, name="Moderator")
    management_role = get(ctx.guild.roles, name="Management")
    staff_role = get(ctx.guild.roles, name="Staff")
    managementNA_role = get(ctx.guild.roles, name="Management NA")
    staffNA_role = get(ctx.guild.roles, name="Staff NA")
    bot_role = get(ctx.guild.roles, name="Bots")
    partners_role = get(ctx.guild.roles, name="Partners")
    buggy_member_ids =[579155972115660803, 131533528616665089, 753029074531909694] 
    async for member in ctx.guild.fetch_members():
        if (not member.bot and member.nick is None and alliance_role not in member.roles and 
            horde_role not in member.roles and pickyourregion_role not in member.roles and 
            client_role not in member.roles and clientNA_role not in member.roles and 
            management_role not in member.roles and staff_role not in member.roles and 
            managementNA_role not in member.roles and staffNA_role not in member.roles and 
            nova_role not in member.roles and moderator_role not in member.roles and 
            bot_role not in member.roles and partners_role not in member.roles and 
            member.id not in buggy_member_ids):
            await member.add_roles(pickyourregion_role)
            await ctx.send(f"{member.name} was given PickYourRegion role")
    await ctx.message.delete()
    await ctx.send("All eligable members have PickYourRegion role or have booster ranks")


@bot.command()
@commands.has_any_role('NOVA', 'Moderator')
async def Logout(ctx):
    await ctx.message.delete()
    await ctx.bot.logout()


@bot.command()
@commands.has_any_role('Bot Whisperer', 'Management')
async def Suspend(ctx, user: discord.Member, duration: float, *, reason: str):
    """To suspend a booster from signing up to boosts
    example: !Suspend @ASLK76#2188 60 for signing up to a boost with wrong roles
    """
    durToDB = datetime.utcnow() + timedelta(minutes=duration)
    HighKeyBoosterA = get(user.roles, name="High Key Booster [A]")
    MBoosterA = get(user.roles, name="M+ Booster [A]")
    ###############################################################
    HighKeyBoosterH = get(user.roles, name="High Key Booster [H]")
    MBoosterH = get(user.roles, name="M+ Booster [H]")
    ###############################################################
    Suspended = get(user.guild.roles, name="Suspended")
    SuspendedH = get(user.guild.roles, name="Suspended {H}")
    suspension_channelA = get(ctx.guild.text_channels, name='suspension')
    suspension_channelH = get(ctx.guild.text_channels, name='suspension-status')
    async with ctx.bot.ops_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await ctx.message.delete()
            if HighKeyBoosterA or HighKeyBoosterH in user.roles:  # High Key Booster Role Check
                if HighKeyBoosterA in user.roles:
                    query = """
                        INSERT INTO suspension (username, role1, role2, role3, duration) 
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    val = (user.id, "High Key Booster [A]", "M+ Booster [A]", "Suspended", durToDB)
                    await cursor.execute(query, val)
                    await user.add_roles(Suspended)
                    await user.remove_roles(HighKeyBoosterA, MBoosterA)
                    durationA_hours = duration / 60
                    await suspension_channelA.send(
                        f"{user.mention} has been Suspended for {durationA_hours:.0f} Hours, {reason}")
                elif HighKeyBoosterH in user.roles:
                    query = """
                        INSERT INTO suspension (username, role1, role2, role3, duration) 
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    val = (user.id, "High Key Booster [H]", "M+ Booster [H]", "Suspended {H}", durToDB)
                    await cursor.execute(query, val)
                    await user.add_roles(SuspendedH)
                    await user.remove_roles(HighKeyBoosterH, MBoosterH)
                    durationH_hours = duration / 60
                    await suspension_channelH.send(
                        f"{user.mention} has been Suspended for {durationH_hours:.0f} Hours, {reason}")
            else:
                if MBoosterA in user.roles:
                    query = """
                        INSERT INTO suspension (username, role2, role3, duration) 
                        VALUES (%s, %s, %s, %s)
                    """
                    val = (user.id, "M+ Booster [A]", "Suspended", durToDB)
                    await cursor.execute(query, val)
                    await user.add_roles(Suspended)
                    await user.remove_roles(MBoosterA)
                    durationA_hours = duration / 60
                    await suspension_channelA.send(
                        f"{user.mention} has been Suspended for {durationA_hours:.0f} Hours, {reason}")
                elif MBoosterH in user.roles:
                    query = """
                        INSERT INTO suspension (username, role2, role3, duration) 
                        VALUES (%s, %s, %s, %s)
                    """
                    val = (user.id, "M+ Booster [H]", "Suspended {H}", durToDB)
                    await cursor.execute(query, val)
                    await user.add_roles(SuspendedH)
                    await user.remove_roles(MBoosterH)
                    durationH_hours = duration / 60
                    await suspension_channelH.send(
                        f"{user.mention} has been Suspended for {durationH_hours:.0f} Hours, {reason}")


@bot.command()
@commands.has_any_role('Bot Whisperer', 'Management')
async def SuspensionCheck(ctx):
    """To manually check the suspension durations
    """
    await ctx.message.delete()
    suspensionA_channel = get(ctx.guild.text_channels, name='suspension')
    suspensionH_channel = get(ctx.guild.text_channels, name='suspension-status')
    HighKeyBoosterA_role = get(ctx.guild.roles, name='High Key Booster [A]')
    MBoosterA_role = get(ctx.guild.roles, name='M+ Booster [A]')
    HighKeyBoosterH_role = get(ctx.guild.roles, name='High Key Booster [H]')
    MBoosterH_role = get(ctx.guild.roles, name='M+ Booster [H]')
    SuspendedA_role = get(ctx.guild.roles, name='Suspended')
    SuspendedH_role = get(ctx.guild.roles, name='Suspended {H}')
    async with ctx.bot.ops_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT * FROM suspension ORDER BY duration desc")
            myresult = await cursor.fetchall()
            for x in myresult:
                if datetime.strptime(x[4], '%Y-%m-%d %H:%M:%S.%f') < datetime.utcnow():
                    member_fromDB = ctx.guild.get_member(x[0])
                    if x[1] == "High Key Booster [A]":
                        await member_fromDB.add_roles(HighKeyBoosterA_role, MBoosterA_role)
                        await member_fromDB.remove_roles(SuspendedA_role)
                        await suspensionA_channel.send(f"{member_fromDB.mention} Suspension has been lifted.")
                    elif x[1] == "High Key Booster [H]":
                        await member_fromDB.add_roles(HighKeyBoosterH_role, MBoosterH_role)
                        await member_fromDB.remove_roles(SuspendedH_role)
                        await suspensionH_channel.send(f"{member_fromDB.mention} Suspension has been lifted.")
                    elif x[1] == "--" and x[2] == "M+ Booster [A]":
                        await member_fromDB.add_roles(MBoosterA_role)
                        await member_fromDB.remove_roles(SuspendedA_role)
                        await suspensionA_channel.send(f"{member_fromDB.mention} Suspension has been lifted.")
                    elif x[1] == "--" and x[2] == "M+ Booster [H]":
                        await member_fromDB.add_roles(MBoosterH_role)
                        await member_fromDB.remove_roles(SuspendedH_role)
                        await suspensionH_channel.send(f"{member_fromDB.mention} Suspension has been lifted.")
                    query = "DELETE FROM suspension WHERE username = %s"
                    val = (x[0],)
                    await cursor.execute(query, val)


@bot.command()
@commands.has_any_role('Bot Whisperer', 'Management')
async def UnSuspend(ctx, user: discord.Member):
    """To manually unsuspend a booster
    example: !UnSuspend @ASLK76#2188
    """
    await ctx.message.delete()
    suspensionA_channel = get(ctx.guild.text_channels, name='suspension')
    suspensionH_channel = get(ctx.guild.text_channels, name='suspension-status')
    HighKeyBoosterA_role = get(ctx.guild.roles, name='High Key Booster [A]')
    MBoosterA_role = get(ctx.guild.roles, name='M+ Booster [A]')
    HighKeyBoosterH_role = get(ctx.guild.roles, name='High Key Booster [H]')
    MBoosterH_role = get(ctx.guild.roles, name='M+ Booster [H]')
    SuspendedA_role = get(ctx.guild.roles, name='Suspended')
    SuspendedH_role = get(ctx.guild.roles, name='Suspended {H}')
    async with ctx.bot.ops_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            query = "SELECT * FROM suspension WHERE username = %s"
            val = (user.id,)
            await cursor.execute(query, val)
            myresult = await cursor.fetchall()
            for x in myresult:
                member_fromDB = ctx.guild.get_member(x[0])
                if x[1] == "High Key Booster [A]":
                    await member_fromDB.add_roles(HighKeyBoosterA_role, MBoosterA_role)
                    await member_fromDB.remove_roles(SuspendedA_role)
                    await suspensionA_channel.send(f"{member_fromDB.mention} Suspension has been lifted.")
                elif x[1] == "High Key Booster [H]":
                    await member_fromDB.add_roles(HighKeyBoosterH_role, MBoosterH_role)
                    await member_fromDB.remove_roles(SuspendedH_role)
                    await suspensionH_channel.send(f"{member_fromDB.mention} Suspension has been lifted.")
                elif x[1] == "--" and x[2] == "M+ Booster [A]":
                    await member_fromDB.add_roles(MBoosterA_role)
                    await member_fromDB.remove_roles(SuspendedA_role)
                    await suspensionA_channel.send(f"{member_fromDB.mention} Suspension has been lifted.")
                elif x[1] == "--" and x[2] == "M+ Booster [H]":
                    await member_fromDB.add_roles(MBoosterH_role)
                    await member_fromDB.remove_roles(SuspendedH_role)
                    await suspensionH_channel.send(f"{member_fromDB.mention} Suspension has been lifted.")
            query = "DELETE FROM suspension WHERE username = %s"
            val = (user.id,)
            await cursor.execute(query, val)


@bot.event
async def on_raw_reaction_add(payload):
    global react_users
    reactionPL = payload.emoji
    channel = bot.get_channel(payload.channel_id)
    message = await channel.fetch_message(payload.message_id)
    guild = bot.get_guild(payload.guild_id)
    user = guild.get_member(payload.user_id)
    hundred_emoji = [reaction for reaction in message.reactions if reaction.emoji == u"\U0001F4AF"]
    moneybag_emoji = [reaction for reaction in message.reactions if reaction.emoji == u"\U0001F4B0"]
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    Staff_role = get(guild.roles, name="Staff")
    Management_role = get(guild.roles, name="Management")
    Nova_role = get(guild.roles, name="NOVA")
    Moderator_role = get(guild.roles, name="Moderator")
    Collector_role = get(guild.roles, name="Collectors")
    CommunitySupport_role = get(guild.roles, name="Community Support")
    Pending_role = get(guild.roles, name='Pending')
    PendingH_role = get(guild.roles, name='Pending [H]')
    Hotshot_A = get(message.guild.roles, name='Hotshot Advertiser [A]')
    Hotshot_H = get(message.guild.roles, name='Hotshot Advertiser [H]')
    roles_to_check = [Staff_role, Management_role, Nova_role, Collector_role, 
                    Moderator_role, CommunitySupport_role]
    roles_check =  any(item in user.roles for item in roles_to_check)
    # region alliance channels
    if ((channel.name.startswith('build-group') or channel.name.startswith('high-keys-group') or 
        (channel.id == 815104637391863857 or channel.name == "🔵leveling-torghast-boost") or 
        (channel.id == 815104639368298545 or channel.name == "🔵rbg-run-submit") or 
        (channel.id == 815104639082823699 or channel.name == "🔵pvp-build-grp")) and 
        reactionPL.name == u"\U0001F513" and not user.bot and roles_check):
        await message.clear_reactions()
        await message.add_reaction(u"\u2705")
        await message.author.remove_roles(Pending_role)
    # endregion

    # region horde channels
    if ((channel.name.startswith('build-grp') or channel.name.startswith('high-keys-grp') or
        (channel.id == 815104637697916959 or channel.name == "🔴leveling-torghast-boost") or 
        (channel.id == 815104639661375488 or channel.name == "🔴rbg-run-submit") or 
        (channel.id == 815104639368298536 or channel.name == "🔴pvp-build-grp")) and 
        reactionPL.name == u"\U0001F513" and not user.bot and roles_check):
        await message.clear_reactions()
        await message.add_reaction(u"\u2705")
        await message.author.remove_roles(PendingH_role)
    # endregion

    if (len(hundred_emoji) == 1 and channel.name == "collectors" and 
        not user.bot and payload.user_id != 163324686086832129):
        await message.remove_reaction(reactionPL, user)

    elif (len(hundred_emoji) == 0 and channel.name == "collectors" and 
        reactionPL.name == u"\u2705" and not user.bot and len(moneybag_emoji) == 0 and 
        len(message.embeds) != 0):
        react_users.append([user.id, now])
        await message.add_reaction(u"\U0001F4B0")

    elif (len(hundred_emoji) == 0 and channel.name == "collectors" and reactionPL.name == u"\U0001F4B0" and 
        user.bot and len(moneybag_emoji) == 1 and len(message.embeds) != 0):
        if len(react_users) == 0:
            await message.remove_reaction(reactionPL, user)
        else:
            def takeSecond(elem):
                return elem[1]
            react_users.sort(key=takeSecond)
            embed_pre = message.embeds[0].to_dict()
            collected_embed = discord.Embed.from_dict(embed_pre)
            collected_embed.add_field(
                    name="**Collected By: **", 
                    value=guild.get_member(react_users[0][0]).mention, 
                    inline=True
                )
            await message.edit(embed=collected_embed)
            react_users.clear()
    
    elif (len(hundred_emoji) == 0 and channel.name == "collectors" and 
        reactionPL.name == u"\U0001F4B0" and not user.bot):
        user_id_pre = None
        user_id = None
        embed_fields = await get_embedded_fields(message)

        if not embed_fields:
            embed_fields = await get_embedded_fields(None, channel=channel, id=message.id)
        if len(embed_fields) < 6:  # if our current message object is not complete, re-try fetching it again
            embed_fields = await get_embedded_fields(None, channel=channel, id=message.id)
        if len(embed_fields) > 5:
            if embed_fields[5]["value"].startswith("<@!"):
                user_id_pre = embed_fields[5]["value"].partition("@!")[2]
                user_id = int(user_id_pre.partition(">")[0])
                user_final = get(guild.members, id=user_id)
            elif embed_fields[5]["value"].startswith("<@"):
                user_id_pre = embed_fields[5]["value"].partition("@")[2]
                user_id = int(user_id_pre.partition(">")[0])
                user_final = get(guild.members, id=user_id)
            if payload.user_id == user_id:
                async with bot.mplus_pool.acquire() as conn:
                    name, realm = await checkPers(user_final.id)
                    if name is not None:
                        collector = f"{name}-{realm}"
                    else:
                        if "-" not in user_final.nick:
                            raise ValueError(f"Nickname format not correct for {user_final}")
                        collector = user_final.nick
                        
                    adv_id_pre = embed_fields[0]["value"].partition("@!")[2]
                    adv_id = int(adv_id_pre.partition(">")[0])
                    adv_final = get(guild.members, id=adv_id)
                    realm = embed_fields[1]["value"].partition(">")[2].strip()
                    amount = embed_fields[2]["value"].partition(">")[2].strip()
                    async with conn.cursor() as cursor:
                        query = """
                            INSERT INTO collectors 
                                (collection_id, collector, trialadv, realm, amount, date_collected) 
                                VALUES (%s, %s, %s, %s, %s, %s)
                        """
                        val = (payload.message_id, collector, adv_final.nick, realm, amount, now)
                        await cursor.execute(query, val)
                        await message.add_reaction(u"\U0001F5C4")
        else:
            await channel.send("I could not find a fifth field. DM Sanfura")
            # remove reaction, or whatever you want to do at this point
            await message.remove_reaction(u"\U0001F4B0", user)

    elif channel.name.startswith("post-run"):
        embed_pre = message.embeds[0].to_dict()
        embed_fields = embed_pre['fields']
        name, realm = await checkPers(payload.user_id)
        if name is not None:
            post_run_nick = f"{name}-{realm}"
        else:
            if "-" not in user.nick:
                raise ValueError(f"Nickname format not correct for {user}")
            post_run_nick = user.nick
        
        if (embed_fields[2]["value"] != post_run_nick and not user.bot and 
            embed_fields[2]["value"] != f"⭐{post_run_nick}⭐" and payload.user_id != 163324686086832129):
            await message.remove_reaction(reactionPL, user)
    
    elif (message.author.id != user.id and not user.bot and 
        (
            channel.name.startswith('build-group') or channel.name.startswith('high-keys-group') or 
            (channel.id == 815104637391863857 or channel.name == "🔵leveling-torghast-boost") or 
            (channel.id == 815104639368298545 or channel.name == "🔵rbg-run-submit") or 
            (channel.id == 815104639082823699 or channel.name == "🔵pvp-build-grp") or 
            channel.name.startswith('build-grp') or channel.name.startswith('high-keys-grp') or
            (channel.id == 815104637697916959 or channel.name == "🔴leveling-torghast-boost") or 
            (channel.id == 815104639661375488 or channel.name == "🔴rbg-run-submit") or 
            (channel.id == 815104639368298536 or channel.name == "🔴pvp-build-grp")
        ) 
        and payload.user_id != 163324686086832129):
        await message.remove_reaction(reactionPL, user)

    elif len(hundred_emoji) == 0 and reactionPL.name == u"\u2705" and (message.author == user or payload.user_id == 163324686086832129) and not user.bot:
        y = message.content.split("\n")
        async with bot.mplus_pool.acquire() as conn:
            # region Alliance build groups
            if (channel.name.startswith('build-group') or channel.name.startswith('high-keys-group')) and \
                (not y[1].startswith('<:house_nova:') and not y[1].startswith('<:inhouse_nova:')) and \
                    Pending_role not in user.roles:
                if not y[3].strip():
                    embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                            color=0x5d4991)
                    embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                    embed_dm.set_footer(text=f"Timestamp: {now}")
                    await user.send(
                        f"Hi **{user.name}**, your input for **__boost pot__** is either "
                        "incomplete or you might have some error in it, please double check "
                        "the pot. If you are sure you didn't do anything wrong, please contact "
                        "Nova Team. Thank you!", 
                        embed=embed_dm)
                    await message.remove_reaction(u"\u2705", user)
                else:
                    pot = convert_si_to_number(y[3].partition(">")[2].replace(",", "."))
                    paid_in = y[2].partition(">")[2].strip()
                    if pot < 999:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, pot cannot be below 1K gold", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif not paid_in or not y[4].strip():
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, your input is missing **__Payment Realm and/or Booster__**, "
                            "please double check. If you are sure you didn't do anything wrong, please contact "
                            "Nova Team. Thank you!", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif paid_in not in realm_name:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, **__`{paid_in}`__** you used is either incomplete or you might have "
                            "some error in it, please double check. If you are sure it's not wrong, please contact "
                            "Nova Team. Thank you!", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    else:
                        adv = message.author
                        adv_name, adv_realm = await checkPers(adv.id)
                        if adv_name is None:
                            adv_result = search_nested_alliance(boosters, adv.nick)
                            if adv_result is not None:
                                adv_name, adv_realm = adv_result.split("-")
                            else:
                                adv_name, adv_realm = adv.nick.split("-")

                        tank_id_pre = y[4].partition("@")[2]
                        if tank_id_pre.startswith("!"):
                            tank_id_pre = tank_id_pre.partition("!")[2]
                        tank_id = int(tank_id_pre.partition(">")[0])
                        tank_user = get(guild.members, id=tank_id)
                        tank_nick = tank_user.nick
                        tank_name, tank_realm = await checkPers(tank_id)
                        if tank_name is None:
                            tank_result = search_nested_alliance(boosters, tank_nick)
                            if tank_result is not None:
                                tank_name, tank_realm = tank_result.split("-")
                            else:
                                tank_name, tank_realm = tank_nick.split("-")

                        healer_id_pre = y[5].partition("@")[2]
                        if healer_id_pre.startswith("!"):
                            healer_id_pre = healer_id_pre.partition("!")[2]
                        healer_id = int(healer_id_pre.partition(">")[0])
                        healer_user = get(guild.members, id=healer_id)
                        healer_nick = healer_user.nick
                        healer_name, healer_realm = await checkPers(healer_id)
                        if healer_name is None:
                            healer_result = search_nested_alliance(boosters, healer_nick)
                            if healer_result is not None:
                                healer_name, healer_realm = healer_result.split("-")
                            else:
                                healer_name, healer_realm = healer_nick.split("-")

                        dps1_id_pre = y[6].partition("@")[2]
                        if dps1_id_pre.startswith("!"):
                            dps1_id_pre = dps1_id_pre.partition("!")[2]
                        dps1_id = int(dps1_id_pre.partition(">")[0])
                        dps1_user = get(guild.members, id=dps1_id)
                        dps1_nick = dps1_user.nick
                        dps1_name, dps1_realm = await checkPers(dps1_id)
                        if dps1_name is None:
                            dps1_result = search_nested_alliance(boosters, dps1_nick)
                            if dps1_result is not None:
                                dps1_name, dps1_realm = dps1_result.split("-")
                            else:
                                dps1_name, dps1_realm = dps1_nick.split("-")

                        dps2_id_pre = y[7].partition("@")[2]
                        if dps2_id_pre.startswith("!"):
                            dps2_id_pre = dps2_id_pre.partition("!")[2]
                        dps2_id = int(dps2_id_pre.partition(">")[0])
                        dps2_user = get(guild.members, id=dps2_id)
                        dps2_nick = dps2_user.nick
                        dps2_name, dps2_realm = await checkPers(dps2_id)
                        if dps2_name is None:
                            dps2_result = search_nested_alliance(boosters, dps2_nick)
                            if dps2_result is not None:
                                dps2_name, dps2_realm = dps2_result.split("-")
                            else:
                                dps2_name, dps2_realm = dps2_nick.split("-")

                        if Hotshot_A not in message.author.roles:
                            adv_cut = int(pot * 0.2)
                        elif Hotshot_A in message.author.roles:
                            adv_cut = int(pot * 0.21)
                        booster_cut = int(pot * 0.175)

                        async with conn.cursor() as cursor:
                            query = """
                                INSERT INTO m_plus 
                                    (boost_faction, boost_id, boost_date, boost_pot, boost_realm,
                                    adv_name, adv_realm, adv_cut, tank_name, tank_realm, tank_cut,
                                    healer_name, healer_realm, healer_cut, dps1_name, dps1_realm, dps1_cut,
                                    dps2_name, dps2_realm, dps2_cut) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """
                            val = ("Alliance", payload.message_id, now, pot, paid_in, adv_name, adv_realm, adv_cut, tank_name,
                                tank_realm, booster_cut, healer_name, healer_realm, booster_cut, dps1_name,
                                dps1_realm, booster_cut, dps2_name, dps2_realm, booster_cut)
                            await cursor.execute(query, val)
                            
                            embed = discord.Embed(title="This run was successfully added to DB.", description="",
                                                color=0x5d4991)
                            embed.add_field(name="**Server**", value=paid_in, inline=True)
                            embed.add_field(name="**POT**",
                                            value=f"{y[3].partition('>')[2].replace(',', '.')} <:goldss:817570131193888828>", 
                                            inline=True)
                            embed.add_field(name="**Advertiser**", 
                                            value=f"{adv_name}-{adv_realm}", 
                                            inline=False)
                            embed.add_field(name="**Advertiser Cut:**",
                                            value=str(adv_cut), inline=True)
                            embed.add_field(name="**Boosters Cut:**",
                                            value=str(booster_cut), inline=True)
                            embed.add_field(name="**Boosters**<:alliance_nova:817570759194968064>",
                                            value=
                                            f"<:tank_nova:817571065207324703> {tank_name} "
                                            f"<:healer_nova:817571133066838016> {healer_name} "
                                            f"<:dps_nova:817571146907385876> {dps1_name} "
                                            f"<:dps_nova:817571146907385876> {dps2_name}", 
                                            inline=False)
                            embed.set_footer(text=f"{now} Run id: {payload.message_id}",
                                            icon_url="https://cdn.discordapp.com/avatars/634917649335320586"
                                            "/e950645a963e4b34502c141b90c2463f.png")
                            log_channel = get(guild.text_channels, id=839436711367933982)
                            await message.add_reaction(u"\U0001F5C4")
                            await log_channel.send(embed=embed)

            elif channel.name == '🔵leveling-torghast-boost' and \
                (not y[1].startswith('<:house_nova:') and not y[1].startswith('<:inhouse_nova:')) and \
                    Pending_role not in user.roles:
                if not y[3].strip():
                    embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                            color=0x5d4991)
                    embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                    embed_dm.set_footer(text=f"Timestamp: {now}")
                    await user.send(
                        f"Hi **{user.name}**, your input for **__boost pot__** is either "
                        "incomplete or you might have some error in it, please double check "
                        "the pot. If you are sure you didn't do anything wrong, please contact "
                        "Nova Team. Thank you!", 
                        embed=embed_dm)
                    await message.remove_reaction(u"\u2705", user)
                else:
                    pot = convert_si_to_number(y[3].partition(">")[2].replace(",", "."))
                    paid_in = y[2].partition(">")[2].strip()
                    if pot < 999:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, pot cannot be below 1K gold", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif not paid_in or not y[4].strip():
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, your input is missing **__Payment Realm and/or Booster__**, "
                            "please double check. If you are sure you didn't do anything wrong, please contact "
                            "Nova Team. Thank you!", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif paid_in not in realm_name:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, **__`{paid_in}`__** you used is either incomplete or you might have "
                            "some error in it, please double check. If you are sure it's not wrong, please contact "
                            "Nova Team. Thank you!", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    else:
                        adv = message.author
                        adv_name, adv_realm = await checkPers(adv.id)
                        if adv_name is None:
                            adv_result = search_nested_alliance(boosters, adv.nick)
                            if adv_result is not None:
                                adv_name, adv_realm = adv_result.split("-")
                            else:
                                adv_name, adv_realm = adv.nick.split("-")

                        tank_id_pre = y[4].partition("@")[2]
                        if tank_id_pre.startswith("!"):
                            tank_id_pre = tank_id_pre.partition("!")[2]
                        tank_id = int(tank_id_pre.partition(">")[0])
                        tank_user = get(guild.members, id=tank_id)
                        tank_nick = tank_user.nick
                        tank_name, tank_realm = await checkPers(tank_id)
                        if tank_name is None:
                            tank_result = search_nested_alliance(boosters, tank_nick)
                            if tank_result is not None:
                                tank_name, tank_realm = tank_result.split("-")
                            else:
                                tank_name, tank_realm = tank_nick.split("-")

                        adv_cut = int(pot * 0.17)
                        booster_cut = int(pot * 0.70)

                        async with conn.cursor() as cursor:
                            query = """
                                INSERT INTO various 
                                    (boost_faction, boost_id, boost_date, boost_pot, boost_realm,
                                    adv_name, adv_realm, adv_cut, tank_name, tank_realm, tank_cut)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """
                            val = ("Alliance", payload.message_id, now, pot, paid_in, adv_name, adv_realm, adv_cut, tank_name,
                                tank_realm, booster_cut)
                            await cursor.execute(query, val)

                            embed = discord.Embed(title="This run was successfully added to DB.", description="",
                                                color=0x5d4991)
                            embed.add_field(name="**Server**", value=paid_in, inline=True)
                            embed.add_field(name="**POT**",
                                            value=f"{y[3].partition('>')[2].replace(',', '.')} <:goldss:817570131193888828>", 
                                            inline=True)
                            embed.add_field(name="**Advertiser**", 
                                            value=f"{adv_name}-{adv_realm}", 
                                            inline=False)
                            embed.add_field(name="**Advertiser Cut:**",
                                            value=str(adv_cut), inline=True)
                            embed.add_field(name="**Boosters Cut:**",
                                            value=str(booster_cut), inline=True)
                            embed.add_field(name="**Boosters**<:alliance_nova:817570759194968064>",
                                            value=f"<:tank_nova:817571065207324703> {tank_name}", 
                                            inline=False)
                            embed.set_footer(text=f"{now} Run id: {payload.message_id}",
                                            icon_url="https://cdn.discordapp.com/avatars/634917649335320586"
                                            "/e950645a963e4b34502c141b90c2463f.png")
                            log_channel = get(guild.text_channels, id=839436711367933982)
                            await message.add_reaction(u"\U0001F5C4")
                            await log_channel.send(embed=embed)

            elif (message.channel.id == 628318833953734676 or channel.name == '🔵pvp-build-grp') and \
                (not y[0].startswith('<:house_nova:') and not y[0].startswith('<:inhouse_nova:')) and \
                    Pending_role not in user.roles:
                if not y[2].strip():
                    embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                            color=0x5d4991)
                    embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                    embed_dm.set_footer(text=f"Timestamp: {now}")
                    await user.send(
                        f"Hi **{user.name}**, your input for **__boost pot__** is either "
                        "incomplete or you might have some error in it, please double check "
                        "the pot. If you are sure you didn't do anything wrong, please contact "
                        "Nova Team. Thank you!", 
                        embed=embed_dm)
                    await message.remove_reaction(u"\u2705", user)
                else:
                    pot = convert_si_to_number(y[2].partition(":")[2].replace(",", "."))
                    paid_in = y[3].partition(":")[2].strip()
                    if pot < 999:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, pot cannot be below 1K gold", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif not paid_in or not y[8].strip():
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, your input is missing **__Payment Realm and/or Booster__**, "
                            "please double check. If you are sure you didn't do anything wrong, please contact "
                            "Nova Team. Thank you!", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif paid_in not in realm_name:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, **__`{paid_in}`__** you used is either incomplete or you might have "
                            "some error in it, please double check. If you are sure it's not wrong, please contact "
                            "Nova Team. Thank you!", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    else:
                        adv = message.author
                        adv_name, adv_realm = await checkPers(adv.id)
                        if adv_name is None:
                            adv_result = search_nested_alliance(boosters, adv.nick)
                            if adv_result is not None:
                                adv_name, adv_realm = adv_result.split("-")
                            else:
                                adv_name, adv_realm = adv.nick.split("-")

                        tank_id_pre = y[8].partition(":")[2].partition("@")[2]
                        if tank_id_pre.startswith("!"):
                            tank_id_pre = tank_id_pre.partition("!")[2]
                        tank_id = int(tank_id_pre.partition(">")[0])
                        tank_user = get(guild.members, id=tank_id)
                        tank_nick = tank_user.nick
                        tank_name, tank_realm = await checkPers(tank_id)
                        if tank_name is None:
                            tank_result = search_nested_alliance(boosters, tank_nick)
                            if tank_result is not None:
                                tank_name, tank_realm = tank_result.split("-")
                            else:
                                tank_name, tank_realm = tank_nick.split("-")
                        
                        adv_cut = int(pot * 0.17)
                        booster_cut = int(pot * 0.70)

                        async with conn.cursor() as cursor:
                            query = """
                                INSERT INTO various 
                                    (boost_faction, boost_id, boost_date, boost_pot, boost_realm,
                                    adv_name, adv_realm, adv_cut, tank_name, tank_realm, tank_cut)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """
                            val = ("Alliance", payload.message_id, now, pot, paid_in, adv_name, adv_realm, adv_cut, tank_name,
                                tank_realm, booster_cut)
                            await cursor.execute(query, val)

                            embed = discord.Embed(title="This run was successfully added to DB.", description="",
                                                color=0x5d4991)
                            embed.add_field(name="**Server**", value=paid_in, inline=True)
                            embed.add_field(name="**POT**",
                                            value=f"{y[2].partition(':')[2].replace(',', '.')} <:goldss:817570131193888828>", 
                                            inline=True)
                            embed.add_field(name="**Advertiser**", 
                                            value=f"{adv_name}-{adv_realm}", 
                                            inline=False)
                            embed.add_field(name="**Advertiser Cut:**",
                                            value=str(adv_cut), inline=True)
                            embed.add_field(name="**Boosters Cut:**",
                                            value=str(booster_cut), inline=True)
                            embed.add_field(name="**Boosters**<:alliance_nova:817570759194968064>",
                                            value=f"<:tank_nova:817571065207324703> {tank_name}", 
                                            inline=False)
                            embed.set_footer(text=f"{now} Run id: {payload.message_id}",
                                            icon_url="https://cdn.discordapp.com/avatars/634917649335320586"
                                            "/e950645a963e4b34502c141b90c2463f.png")
                            log_channel = get(guild.text_channels, id=839436711367933982)
                            await message.add_reaction(u"\U0001F5C4")
                            await log_channel.send(embed=embed)

            elif channel.name.startswith('🔵rbg-run-submit') and \
                (not y[1].startswith('<:house_nova:') and not y[1].startswith('<:inhouse_nova:')) and \
                    Pending_role not in user.roles:
                if not y[3].strip():
                    embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                            color=0x5d4991)
                    embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                    embed_dm.set_footer(text=f"Timestamp: {now}")
                    await user.send(
                        f"Hi **{user.name}**, your input for **__boost pot__** is either "
                        "incomplete or you might have some error in it, please double check "
                        "the pot. If you are sure you didn't do anything wrong, please contact "
                        "Nova Team. Thank you!", 
                        embed=embed_dm)
                    await message.remove_reaction(u"\u2705", user)
                else:
                    pot = convert_si_to_number(y[3].partition(">")[2].replace(",", "."))
                    paid_in = y[2].partition(">")[2]
                    paid_in = paid_in.strip()
                    if pot < 999:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, pot cannot be below 1K gold", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif not paid_in or not y[4].strip():
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, your input is missing **__Payment Realm and/or Booster__**, "
                            "please double check. If you are sure you didn't do anything wrong, please contact "
                            "Nova Team. Thank you!", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif paid_in not in realm_name:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, **__`{paid_in}`__** you used is either incomplete or you might have "
                            "some error in it, please double check. If you are sure it's not wrong, please contact "
                            "Nova Team. Thank you!", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    else:
                        adv = message.author
                        adv_name, adv_realm = await checkPers(adv.id)
                        if adv_name is None:
                            adv_result = search_nested_alliance(boosters, adv.nick)
                            if adv_result is not None:
                                adv_name, adv_realm = adv_result.split("-")
                            else:
                                adv_name, adv_realm = adv.nick.split("-")

                        tank_id_pre = y[4].partition("@")[2]
                        if tank_id_pre.startswith("!"):
                            tank_id_pre = tank_id_pre.partition("!")[2]
                        tank_id = int(tank_id_pre.partition(">")[0])
                        tank_user = get(guild.members, id=tank_id)
                        tank_nick = tank_user.nick
                        tank_name, tank_realm = await checkPers(tank_id)
                        if tank_name is None:
                            tank_result = search_nested_alliance(boosters, tank_nick)
                            if tank_result is not None:
                                tank_name, tank_realm = tank_result.split("-")
                            else:
                                tank_name, tank_realm = tank_nick.split("-")


                        adv_cut = int(pot * 0.10)
                        booster_cut = int(pot * 0.8)

                        async with conn.cursor() as cursor:
                            query = """
                                INSERT INTO various 
                                    (boost_faction, boost_id, boost_date, boost_pot, boost_realm,
                                    adv_name, adv_realm, adv_cut, tank_name, tank_realm, tank_cut)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """
                            val = ("Alliance", payload.message_id, now, pot, paid_in, adv_name, adv_realm, adv_cut, tank_name,
                                tank_realm, booster_cut)
                            await cursor.execute(query, val)

                            embed = discord.Embed(title="This run was successfully added to DB.", description="",
                                                color=0x5d4991)
                            embed.add_field(name="**Server**", value=paid_in, inline=True)
                            embed.add_field(name="**POT**",
                                            value=f"{y[3].partition('>')[2].replace(',', '.')} <:goldss:817570131193888828>", 
                                            inline=True)
                            embed.add_field(name="**Advertiser**", 
                                            value=f"{adv_name}-{adv_realm}", 
                                            inline=False)
                            embed.add_field(name="**Advertiser Cut:**",
                                            value=str(adv_cut), inline=True)
                            embed.add_field(name="**Boosters Cut:**",
                                            value=str(booster_cut), inline=True)
                            embed.add_field(name="**Boosters**<:alliance_nova:817570759194968064>",
                                            value=f"<:tank_nova:817571065207324703> {tank_name}", 
                                            inline=False)
                            embed.set_footer(text=f"{now} Run id: {payload.message_id}",
                                            icon_url="https://cdn.discordapp.com/avatars/634917649335320586"
                                            "/e950645a963e4b34502c141b90c2463f.png")
                            log_channel = get(guild.text_channels, id=839436711367933982)
                            await message.add_reaction(u"\U0001F5C4")
                            await log_channel.send(embed=embed)
            # endregion
            # region Horde build groups
            elif (channel.name.startswith('build-grp') or channel.name.startswith('high-keys-grp')) and \
                (not y[1].startswith('<:house_nova:') and not y[1].startswith('<:inhouse_nova:')) and \
                    PendingH_role not in user.roles:
                if not y[3].strip():
                    embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                            color=0x5d4991)
                    embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                    embed_dm.set_footer(text=f"Timestamp: {now}")
                    await user.send(
                        f"Hi **{user.name}**, your input for **__boost pot__** is either "
                        "incomplete or you might have some error in it, please double check "
                        "the pot. If you are sure you didn't do anything wrong, please contact "
                        "Nova Team. Thank you!", 
                        embed=embed_dm)
                    await message.remove_reaction(u"\u2705", user)
                else:
                    pot = convert_si_to_number(y[3].partition(">")[2].replace(",", "."))
                    paid_in = y[2].partition(">")[2].strip()
                    if pot < 999:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, pot cannot be below 1K gold", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif not paid_in or not y[4].strip():
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, your input is missing **__Payment Realm and/or Booster__**, "
                            "please double check. If you are sure you didn't do anything wrong, please contact "
                            "Nova Team. Thank you!", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif paid_in not in realm_name:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, **__`{paid_in}`__** you used is either incomplete or you might have "
                            "some error in it, please double check. If you are sure it's not wrong, please contact "
                            "Nova Team. Thank you!", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    else:
                        adv = message.author
                        adv_name, adv_realm = await checkPers(adv.id)
                        if adv_name is None:
                            adv_result = search_nested_horde(boosters, adv.nick)
                            if adv_result is not None:
                                adv_name, adv_realm = adv_result.split("-")
                            else:
                                adv_name, adv_realm = adv.nick.split("-")

                        tank_id_pre = y[4].partition("@")[2]
                        if tank_id_pre.startswith("!"):
                            tank_id_pre = tank_id_pre.partition("!")[2]
                        tank_id = int(tank_id_pre.partition(">")[0])
                        tank_user = get(guild.members, id=tank_id)
                        tank_nick = tank_user.nick
                        tank_name, tank_realm = await checkPers(tank_id)
                        if tank_name is None:
                            tank_result = search_nested_horde(boosters, tank_nick)
                            if tank_result is not None:
                                tank_name, tank_realm = tank_result.split("-")
                            else:
                                tank_name, tank_realm = tank_nick.split("-")

                        healer_id_pre = y[5].partition("@")[2]
                        if healer_id_pre.startswith("!"):
                            healer_id_pre = healer_id_pre.partition("!")[2]
                        healer_id = int(healer_id_pre.partition(">")[0])
                        healer_user = get(guild.members, id=healer_id)
                        healer_nick = healer_user.nick
                        healer_name, healer_realm = await checkPers(healer_id)
                        if healer_name is None:
                            healer_result = search_nested_horde(boosters, healer_nick)
                            if healer_result is not None:
                                healer_name, healer_realm = healer_result.split("-")
                            else:
                                healer_name, healer_realm = healer_nick.split("-")

                        dps1_id_pre = y[6].partition("@")[2]
                        if dps1_id_pre.startswith("!"):
                            dps1_id_pre = dps1_id_pre.partition("!")[2]
                        dps1_id = int(dps1_id_pre.partition(">")[0])
                        dps1_user = get(guild.members, id=dps1_id)
                        dps1_nick = dps1_user.nick
                        dps1_name, dps1_realm = await checkPers(dps1_id)
                        if dps1_name is None:
                            dps1_result = search_nested_horde(boosters, dps1_nick)
                            if dps1_result is not None:
                                dps1_name, dps1_realm = dps1_result.split("-")
                            else:
                                dps1_name, dps1_realm = dps1_nick.split("-")

                        dps2_id_pre = y[7].partition("@")[2]
                        if dps2_id_pre.startswith("!"):
                            dps2_id_pre = dps2_id_pre.partition("!")[2]
                        dps2_id = int(dps2_id_pre.partition(">")[0])
                        dps2_user = get(guild.members, id=dps2_id)
                        dps2_nick = dps2_user.nick
                        dps2_name, dps2_realm = await checkPers(dps2_id)
                        if dps2_name is None:
                            dps2_result = search_nested_horde(boosters, dps2_nick)
                            if dps2_result is not None:
                                dps2_name, dps2_realm = dps2_result.split("-")
                            else:
                                dps2_name, dps2_realm = dps2_nick.split("-")

                        if Hotshot_H not in message.author.roles:
                            adv_cut = int(pot * 0.17)
                        elif Hotshot_H in message.author.roles:
                            adv_cut = int(pot * 0.21)
                        booster_cut = int(pot * 0.175)

                        async with conn.cursor() as cursor:
                            query = """
                                INSERT INTO m_plus 
                                    (boost_faction, boost_id, boost_date, boost_pot, boost_realm,
                                    adv_name, adv_realm, adv_cut, tank_name, tank_realm, tank_cut,
                                    healer_name, healer_realm, healer_cut, dps1_name, dps1_realm, dps1_cut,
                                    dps2_name, dps2_realm, dps2_cut) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """
                            val = ("Horde", payload.message_id, now, pot, paid_in, adv_name, adv_realm, adv_cut, tank_name,
                                tank_realm, booster_cut, healer_name, healer_realm, booster_cut, dps1_name,
                                dps1_realm, booster_cut, dps2_name, dps2_realm, booster_cut)
                            await cursor.execute(query, val)

                            embed = discord.Embed(title="This run was successfully added to DB.", description="",
                                                    color=0x5d4991)
                            embed.add_field(name="**Server**", value=paid_in, inline=True)
                            embed.add_field(name="**POT**",
                                            value=f"{y[3].partition('>')[2].replace(',', '.')} <:goldss:817570131193888828>", 
                                            inline=True)
                            embed.add_field(name="**Advertiser**", 
                                            value=f"{adv_name}-{adv_realm}", 
                                            inline=False)
                            embed.add_field(name="**Advertiser Cut:**",
                                            value=str(adv_cut), inline=True)
                            embed.add_field(name="**Boosters Cut:**",
                                            value=str(booster_cut), inline=True)
                            embed.add_field(name="**Boosters**<:horde_nova:817556558435188747>",
                                            value=
                                            f"<:tank_nova:817571065207324703> {tank_name} "
                                            f"<:healer_nova:817571133066838016> {healer_name} "
                                            f"<:dps_nova:817571146907385876> {dps1_name} "
                                            f"<:dps_nova:817571146907385876> {dps2_name}", 
                                            inline=False)
                            embed.set_footer(text=f"{now} Run id: {payload.message_id}",
                                            icon_url="https://cdn.discordapp.com/avatars/634917649335320586"
                                            "/e950645a963e4b34502c141b90c2463f.png")
                            log_channel = get(guild.text_channels, id=839436711367933982)
                            await message.add_reaction(u"\U0001F5C4")
                            await log_channel.send(embed=embed)

            elif channel.name == '🔴leveling-torghast-boost' and \
                (not y[1].startswith('<:house_nova:') and not y[1].startswith('<:inhouse_nova:')) and \
                    PendingH_role not in user.roles:
                if not y[3].strip():
                    embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                            color=0x5d4991)
                    embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                    embed_dm.set_footer(text=f"Timestamp: {now}")
                    await user.send(
                        f"Hi **{user.name}**, your input for **__boost pot__** is either "
                        "incomplete or you might have some error in it, please double check "
                        "the pot. If you are sure you didn't do anything wrong, please contact "
                        "Nova Team. Thank you!", 
                        embed=embed_dm)
                    await message.remove_reaction(u"\u2705", user)
                else:
                    pot = convert_si_to_number(y[3].partition(">")[2].replace(",", "."))
                    paid_in = y[2].partition(">")[2].strip()
                    if pot < 999:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, pot cannot be below 1K gold", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif not paid_in or not y[4].strip():
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, your input is missing **__Payment Realm and/or Booster__**, "
                            "please double check. If you are sure you didn't do anything wrong, please contact "
                            "Nova Team. Thank you!", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif paid_in not in realm_name:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, **__`{paid_in}`__** you used is either incomplete or you might have "
                            "some error in it, please double check. If you are sure it's not wrong, please contact "
                            "Nova Team. Thank you!", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    else:
                        adv = message.author
                        adv_name, adv_realm = await checkPers(adv.id)
                        if adv_name is None:
                            adv_result = search_nested_horde(boosters, adv.nick)
                            if adv_result is not None:
                                adv_name, adv_realm = adv_result.split("-")
                            else:
                                adv_name, adv_realm = adv.nick.split("-")

                        tank_id_pre = y[4].partition("@")[2]
                        if tank_id_pre.startswith("!"):
                            tank_id_pre = tank_id_pre.partition("!")[2]
                        tank_id = int(tank_id_pre.partition(">")[0])
                        tank_user = get(guild.members, id=tank_id)
                        tank_nick = tank_user.nick
                        tank_name, tank_realm = await checkPers(tank_id)
                        if tank_name is None:
                            tank_result = search_nested_horde(boosters, tank_nick)
                            if tank_result is not None:
                                tank_name, tank_realm = tank_result.split("-")
                            else:
                                tank_name, tank_realm = tank_nick.split("-")

                        adv_cut = int(pot * 0.17)
                        booster_cut = int(pot * 0.70)

                        async with conn.cursor() as cursor:
                            query = """
                                INSERT INTO various 
                                    (boost_faction, boost_id, boost_date, boost_pot, boost_realm,
                                    adv_name, adv_realm, adv_cut, tank_name, tank_realm, tank_cut)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """
                            val = ("Horde", payload.message_id, now, pot, paid_in, adv_name, adv_realm, adv_cut, tank_name,
                                tank_realm, booster_cut)
                            await cursor.execute(query, val)

                            embed = discord.Embed(title="This run was successfully added to DB.", description="",
                                                color=0x5d4991)
                            embed.add_field(name="**Server**", value=paid_in, inline=True)
                            embed.add_field(name="**POT**",
                                            value=f"{y[3].partition('>')[2].replace(',', '.')} <:goldss:817570131193888828>", 
                                            inline=True)
                            embed.add_field(name="**Advertiser**", 
                                            value=f"{adv_name}-{adv_realm}", 
                                            inline=False)
                            embed.add_field(name="**Advertiser Cut:**",
                                            value=str(adv_cut), inline=True)
                            embed.add_field(name="**Boosters Cut:**",
                                            value=str(booster_cut), inline=True)
                            embed.add_field(name="**Boosters**<:horde_nova:817556558435188747>",
                                            value=f"<:tank_nova:817571065207324703> {tank_name}", 
                                            inline=False)
                            embed.set_footer(text=f"{now} Run id: {payload.message_id}",
                                            icon_url="https://cdn.discordapp.com/avatars/634917649335320586"
                                            "/e950645a963e4b34502c141b90c2463f.png")
                            log_channel = get(guild.text_channels, id=839436711367933982)
                            await message.add_reaction(u"\U0001F5C4")
                            await log_channel.send(embed=embed)

            elif (message.channel.id == 714121043924484146 or channel.name == '🔴pvp-build-grp') and \
                (not y[0].startswith('<:house_nova:') and not y[0].startswith('<:inhouse_nova:')) and \
                    PendingH_role not in user.roles:
                if not y[2].strip():
                    embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                            color=0x5d4991)
                    embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                    embed_dm.set_footer(text=f"Timestamp: {now}")
                    await user.send(
                        f"Hi **{user.name}**, your input for **__boost pot__** is either "
                        "incomplete or you might have some error in it, please double check "
                        "the pot. If you are sure you didn't do anything wrong, please contact "
                        "Nova Team. Thank you!", 
                        embed=embed_dm)
                    await message.remove_reaction(u"\u2705", user)
                else:
                    pot = convert_si_to_number(y[2].partition(":")[2].replace(",", "."))
                    paid_in = y[3].partition(":")[2].strip()
                    if pot < 999:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, pot cannot be below 1K gold", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif not paid_in or not y[8].strip():
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, your input is missing **__Payment Realm and/or Booster__**, "
                            "please double check. If you are sure you didn't do anything wrong, please contact "
                            "Nova Team. Thank you!", 
                            embed=embed_dm)
                    elif paid_in not in realm_name:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, **__`{paid_in}`__** you used is either incomplete or you might have "
                            "some error in it, please double check. If you are sure it's not wrong, please contact "
                            "Nova Team. Thank you!", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    else:
                        adv = message.author
                        adv_name, adv_realm = await checkPers(adv.id)
                        if adv_name is None:
                            adv_result = search_nested_horde(boosters, adv.nick)
                            if adv_result is not None:
                                adv_name, adv_realm = adv_result.split("-")
                            else:
                                adv_name, adv_realm = adv.nick.split("-")

                        tank_id_pre = y[8].partition(":")[2].partition("@")[2]
                        if tank_id_pre.startswith("!"):
                            tank_id_pre = tank_id_pre.partition("!")[2]
                        tank_id = int(tank_id_pre.partition(">")[0])
                        tank_user = get(guild.members, id=tank_id)
                        tank_nick = tank_user.nick
                        tank_name, tank_realm = await checkPers(tank_id)
                        if tank_name is None:
                            tank_result = search_nested_horde(boosters, tank_nick)
                            if tank_result is not None:
                                tank_name, tank_realm = tank_result.split("-")
                            else:
                                tank_name, tank_realm = tank_nick.split("-")

                        adv_cut = int(pot * 0.17)
                        booster_cut = int(pot * 0.70)

                        async with conn.cursor() as cursor:
                            query = """
                                INSERT INTO various 
                                    (boost_faction, boost_id, boost_date, boost_pot, boost_realm,
                                    adv_name, adv_realm, adv_cut, tank_name, tank_realm, tank_cut)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """
                            val = ("Horde", payload.message_id, now, pot, paid_in, adv_name, adv_realm, adv_cut, tank_name,
                                tank_realm, booster_cut)
                            await cursor.execute(query, val)

                            embed = discord.Embed(title="This run was successfully added to DB.", description="",
                                                color=0x5d4991)
                            embed.add_field(name="**Server**", value=paid_in, inline=True)
                            embed.add_field(name="**POT**",
                                            value=f"{y[2].partition(':')[2].replace(',', '.')} <:goldss:817570131193888828>", 
                                            inline=True)
                            embed.add_field(name="**Advertiser**", 
                                            value=f"{adv_name}-{adv_realm}", 
                                            inline=False)
                            embed.add_field(name="**Advertiser Cut:**",
                                            value=str(adv_cut), inline=True)
                            embed.add_field(name="**Boosters Cut:**",
                                            value=str(booster_cut), inline=True)
                            embed.add_field(name="**Boosters**<:horde_nova:817556558435188747>",
                                            value=f"<:tank_nova:817571065207324703> {tank_name}", 
                                            inline=False)
                            embed.set_footer(text=f"{now} Run id: {payload.message_id}",
                                            icon_url="https://cdn.discordapp.com/avatars/634917649335320586"
                                            "/e950645a963e4b34502c141b90c2463f.png")
                            log_channel = get(guild.text_channels, id=839436711367933982)
                            await message.add_reaction(u"\U0001F5C4")
                            await log_channel.send(embed=embed)

            elif channel.name.startswith('🔴rbg-run-submit') and \
                (not y[1].startswith('<:house_nova:') and not y[1].startswith('<:inhouse_nova:')) and \
                    PendingH_role not in user.roles:
                if not y[3].strip():
                    embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                            color=0x5d4991)
                    embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                    embed_dm.set_footer(text=f"Timestamp: {now}")
                    await user.send(
                        f"Hi **{user.name}**, your input for **__boost pot__** is either "
                        "incomplete or you might have some error in it, please double check "
                        "the pot. If you are sure you didn't do anything wrong, please contact "
                        "Nova Team. Thank you!", 
                        embed=embed_dm)
                    await message.remove_reaction(u"\u2705", user)
                else:
                    pot = convert_si_to_number(y[3].partition(">")[2].replace(",", "."))
                    paid_in = y[2].partition(">")[2]
                    paid_in = paid_in.strip()
                    if pot < 999:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, pot cannot be below 1K gold", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif not paid_in or not y[4].strip():
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, your input is missing **__Payment Realm and/or Booster__**, "
                            "please double check. If you are sure you didn't do anything wrong, please contact "
                            "Nova Team. Thank you!", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif paid_in not in realm_name:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        await user.send(
                            f"Hi **{user.name}**, **__`{paid_in}`__** you used is either incomplete or you might have "
                            "some error in it, please double check. If you are sure it's not wrong, please contact "
                            "Nova Team. Thank you!", 
                            embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    else:
                        adv = message.author
                        adv_name, adv_realm = await checkPers(adv.id)
                        if adv_name is None:
                            adv_result = search_nested_horde(boosters, adv.nick)
                            if adv_result is not None:
                                adv_name, adv_realm = adv_result.split("-")
                            else:
                                adv_name, adv_realm = adv.nick.split("-")

                        tank_id_pre = y[4].partition("@")[2]
                        if tank_id_pre.startswith("!"):
                            tank_id_pre = tank_id_pre.partition("!")[2]
                        tank_id = int(tank_id_pre.partition(">")[0])
                        tank_user = get(guild.members, id=tank_id)
                        tank_nick = tank_user.nick
                        tank_name, tank_realm = await checkPers(tank_id)
                        if tank_name is None:
                            tank_result = search_nested_horde(boosters, tank_nick)
                            if tank_result is not None:
                                tank_name, tank_realm = tank_result.split("-")
                            else:
                                tank_name, tank_realm = tank_nick.split("-")

                        adv_cut = int(pot * 0.10)
                        booster_cut = int(pot * 0.8)

                        async with conn.cursor() as cursor:
                            query = """
                                INSERT INTO various 
                                    (boost_faction, boost_id, boost_date, boost_pot, boost_realm,
                                    adv_name, adv_realm, adv_cut, tank_name, tank_realm, tank_cut)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """
                            val = ("Horde", payload.message_id, now, pot, paid_in, adv_name, adv_realm, adv_cut, tank_name,
                                tank_realm, booster_cut)
                            await cursor.execute(query, val)

                            embed = discord.Embed(title="This run was successfully added to DB.", description="",
                                                color=0x5d4991)
                            embed.add_field(name="**Server**", value=paid_in, inline=True)
                            embed.add_field(name="**POT**",
                                            value=f"{y[3].partition('>')[2].replace(',', '.')} <:goldss:817570131193888828>", 
                                            inline=True)
                            embed.add_field(name="**Advertiser**", 
                                            value=f"{adv_name}-{adv_realm}", 
                                            inline=False)
                            embed.add_field(name="**Advertiser Cut:**",
                                            value=str(adv_cut), inline=True)
                            embed.add_field(name="**Boosters Cut:**",
                                            value=str(booster_cut), inline=True)
                            embed.add_field(name="**Boosters**<:horde_nova:817556558435188747>",
                                                value=f"<:tank_nova:817571065207324703> {tank_name}", 
                                            inline=False)
                            embed.set_footer(text=f"{now} Run id: {payload.message_id}",
                                            icon_url="https://cdn.discordapp.com/avatars/634917649335320586"
                                            "/e950645a963e4b34502c141b90c2463f.png")
                            log_channel = get(guild.text_channels, id=839436711367933982)
                            await message.add_reaction(u"\U0001F5C4")
                            await log_channel.send(embed=embed)
            # endregion
            
            # ########### INHOUSE AND CLIENTS ############
            # region Alliance build groups
            if (channel.name.startswith('build-group') or channel.name.startswith('high-keys-group')) and \
                (y[1].startswith('<:house_nova:') or y[1].startswith('<:inhouse_nova:')) and \
                    Pending_role not in user.roles:
                if not y[4].strip():
                    embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                            color=0x5d4991)
                    embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                    embed_dm.set_footer(text=f"Timestamp: {now}")
                    
                    await user.send(
                        f"Hi **{user.name}**, your input for **__boost pot__** is either incomplete or you might have some error on "
                        "it, please double check the pot. If you are sure you "
                        "didn't do anything wrong, please contact Nova Team. Thank you!", embed=embed_dm)
                    await message.remove_reaction(u"\u2705", user)
                else:
                    pot = convert_si_to_number(y[4].partition(">")[2].replace(",", "."))
                    paid_in = y[3].partition(">")[2].strip()
                    if pot < 999:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, pot cannot be below 1K gold", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif not paid_in or not y[5].strip():
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, your input is missing **__Payment Realm and/or Booster__**, please double check."
                            "If you are sure you didn't do anything wrong, please contact Nova Team. Thank you!", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif paid_in not in realm_name:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, **__`{paid_in}`__** you used is either incomplete or you might have "
                            "some error in it, please double check. If you are sure it's not wrong, please contact "
                            "Nova Team. Thank you!", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    else:
                        adv = message.author
                        adv_name, adv_realm = await checkPers(adv.id)
                        if adv_name is None:
                            adv_result = search_nested_alliance(boosters, adv.nick)
                            if adv_result is not None:
                                adv_name, adv_realm = adv_result.split("-")
                            else:
                                adv_name, adv_realm = adv.nick.split("-")
                        # adv = message.author.nick
                        ########################
                        tank_id_pre = y[5].partition("@")[2]
                        if tank_id_pre.startswith("!"):
                            tank_id_pre = tank_id_pre.partition("!")[2]
                        tank_id = int(tank_id_pre.partition(">")[0])
                        tank_user = get(guild.members, id=tank_id)
                        tank_nick = tank_user.nick
                        tank_name, tank_realm = await checkPers(tank_id)
                        if tank_name is None:
                            tank_result = search_nested_alliance(boosters, tank_nick)
                            if tank_result is not None:
                                tank_name, tank_realm = tank_result.split("-")
                            else:
                                tank_name, tank_realm = tank_nick.split("-")

                        ##############################################
                        healer_id_pre = y[6].partition("@")[2]
                        if healer_id_pre.startswith("!"):
                            healer_id_pre = healer_id_pre.partition("!")[2]
                        healer_id = int(healer_id_pre.partition(">")[0])
                        healer_user = get(guild.members, id=healer_id)
                        healer_nick = healer_user.nick
                        healer_name, healer_realm = await checkPers(healer_id)
                        if healer_name is None:
                            healer_result = search_nested_alliance(boosters, healer_nick)
                            if healer_result is not None:
                                healer_name, healer_realm = healer_result.split("-")
                            else:
                                healer_name, healer_realm = healer_nick.split("-")

                        dps1_id_pre = y[7].partition("@")[2]
                        if dps1_id_pre.startswith("!"):
                            dps1_id_pre = dps1_id_pre.partition("!")[2]
                        dps1_id = int(dps1_id_pre.partition(">")[0])
                        dps1_user = get(guild.members, id=dps1_id)
                        dps1_nick = dps1_user.nick
                        dps1_name, dps1_realm = await checkPers(dps1_id)
                        if dps1_name is None:
                            dps1_result = search_nested_alliance(boosters, dps1_nick)
                            if dps1_result is not None:
                                dps1_name, dps1_realm = dps1_result.split("-")
                            else:
                                dps1_name, dps1_realm = dps1_nick.split("-")

                        dps2_id_pre = y[8].partition("@")[2]
                        if dps2_id_pre.startswith("!"):
                            dps2_id_pre = dps2_id_pre.partition("!")[2]
                        dps2_id = int(dps2_id_pre.partition(">")[0])
                        dps2_user = get(guild.members, id=dps2_id)
                        dps2_nick = dps2_user.nick
                        dps2_name, dps2_realm = await checkPers(dps2_id)
                        if dps2_name is None:
                            dps2_result = search_nested_alliance(boosters, dps2_nick)
                            if dps2_result is not None:
                                dps2_name, dps2_realm = dps2_result.split("-")
                            else:
                                dps2_name, dps2_realm = dps2_nick.split("-")

                        ##############################################
                        
                        if y[1].startswith('<:house_nova:'):
                            adv_cut = int(pot * 0.10)
                        elif y[1].startswith('<:inhouse_nova:'):
                            adv_cut = int(pot * 0.07)
                        booster_cut = int(pot * 0.175)
                        async with conn.cursor() as cursor:
                            query = """
                            INSERT INTO m_plus 
                            (boost_faction, boost_id, boost_date, boost_pot, boost_realm,
                                    adv_name, adv_realm, adv_cut, tank_name, tank_realm, tank_cut,
                                    healer_name, healer_realm, healer_cut, dps1_name, dps1_realm, dps1_cut,
                                    dps2_name, dps2_realm, dps2_cut) VALUES (%s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            val = ("Alliance", payload.message_id, now, pot, paid_in, adv_name, adv_realm, adv_cut, tank_name,
                                tank_realm, booster_cut, healer_name, healer_realm, booster_cut, dps1_name,
                                dps1_realm, booster_cut, dps2_name, dps2_realm, booster_cut)
                            await cursor.execute(query, val)

                            embed = discord.Embed(title="This run was successfully added to DB.", description="",
                                                color=0x5d4991)
                            embed.add_field(name="**Server**", value=paid_in, inline=True)
                            embed.add_field(name="**POT**",
                                            value=y[4].partition(">")[2] + "<:goldss:817570131193888828>", inline=True)
                            embed.add_field(name="**Advertiser**", value=adv_name +
                                            "-" + adv_realm, inline=False)
                            embed.add_field(name="**Advertiser Cut:**",
                                            value=str(adv_cut), inline=True)
                            embed.add_field(name="**Boosters Cut:**",
                                            value=str(booster_cut), inline=True)
                            embed.add_field(name="**Boosters**<:alliance_nova:817570759194968064>",
                                            value="<:tank_nova:817571065207324703>" + tank_name + " <:healer_nova:817571133066838016>" + healer_name + " <:dps_nova:817571146907385876>" + dps1_name + " <:dps_nova:817571146907385876>" + dps2_name, inline=False)
                            embed.set_footer(text=f"{now} Run id: {payload.message_id}",
                                            icon_url="https://cdn.discordapp.com/avatars/634917649335320586"
                                            "/e950645a963e4b34502c141b90c2463f.png")
                            log_channel = get(guild.text_channels, id=839436711367933982)
                            await message.add_reaction(u"\U0001F5C4")
                            await log_channel.send(embed=embed)

            elif channel.name == '🔵leveling-torghast-boost' and \
                (y[1].startswith('<:house_nova:') or y[1].startswith('<:inhouse_nova:')) and \
                    Pending_role not in user.roles:
                if not y[4].strip():
                    embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                            color=0x5d4991)
                    embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                    embed_dm.set_footer(text=f"Timestamp: {now}")
                    
                    await user.send(
                        f"Hi **{user.name}**, your input for **__boost pot__** is either incomplete or you might have some error on "
                        "it, please double check the pot. If you are sure you "
                        "didn't do anything wrong, please contact Nova Team. Thank you!", embed=embed_dm)
                    await message.remove_reaction(u"\u2705", user)
                else:
                    pot = convert_si_to_number(y[4].partition(">")[2].replace(",", "."))
                    paid_in = y[3].partition(">")[2].strip()
                    if pot < 999:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, pot cannot be below 1K gold", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif not paid_in or not y[5].strip():
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, your input is missing **__Payment Realm and/or Booster__**, please double check."
                            "If you are sure you didn't do anything wrong, please contact Nova Team. Thank you!", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif paid_in not in realm_name:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, **__`{paid_in}`__** you used is either incomplete or you might have "
                            "some error in it, please double check. If you are sure it's not wrong, please contact "
                            "Nova Team. Thank you!", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    else:
                        adv = message.author
                        adv_name, adv_realm = await checkPers(adv.id)
                        if adv_name is None:
                            adv_result = search_nested_alliance(boosters, adv.nick)
                            if adv_result is not None:
                                adv_name, adv_realm = adv_result.split("-")
                            else:
                                adv_name, adv_realm = adv.nick.split("-")
                        ###########################################################
                        tank_id_pre = y[5].partition("@")[2]
                        if tank_id_pre.startswith("!"):
                            tank_id_pre = tank_id_pre.partition("!")[2]
                        tank_id = int(tank_id_pre.partition(">")[0])
                        tank_user = get(guild.members, id=tank_id)
                        tank_nick = tank_user.nick
                        tank_name, tank_realm = await checkPers(tank_id)
                        if tank_name is None:
                            tank_result = search_nested_alliance(boosters, tank_nick)
                            if tank_result is not None:
                                tank_name, tank_realm = tank_result.split("-")
                            else:
                                tank_name, tank_realm = tank_nick.split("-")
                        #########################################################
                        if y[1].startswith('<:house_nova:'):
                            adv_cut = int(pot * 0.10)
                        elif y[1].startswith('<:inhouse_nova:'):
                            adv_cut = int(pot * 0.07)
                        booster_cut = int(pot * 0.7)

                        async with conn.cursor() as cursor:
                            query = """INSERT INTO various (boost_faction, boost_id, boost_date, boost_pot, boost_realm,
                                    adv_name, adv_realm, adv_cut, tank_name, tank_realm, tank_cut)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            val = ("Alliance", payload.message_id, now, pot, paid_in, adv_name, adv_realm, adv_cut, tank_name,
                                tank_realm, booster_cut)
                            await cursor.execute(query, val)

                            embed = discord.Embed(title="This run was successfully added to DB.", description="",
                                                color=0x5d4991)
                            embed.add_field(name="**Server**", value=paid_in, inline=True)
                            embed.add_field(name="**POT**",
                                            value=y[4].partition(">")[2] + "<:goldss:817570131193888828>", inline=True)
                            embed.add_field(name="**Advertiser**", value=adv_name +
                                            "-" + adv_realm, inline=False)
                            embed.add_field(name="**Advertiser Cut:**",
                                            value=str(adv_cut), inline=True)
                            embed.add_field(name="**Boosters Cut:**",
                                            value=str(booster_cut), inline=True)
                            embed.add_field(name="**Boosters**<:alliance_nova:817570759194968064>",
                                            value="<:tank_nova:817571065207324703>" + tank_name, inline=False)
                            embed.set_footer(text=f"{now} Run id: {payload.message_id}",
                                            icon_url="https://cdn.discordapp.com/avatars/634917649335320586"
                                            "/e950645a963e4b34502c141b90c2463f.png")
                            log_channel = get(guild.text_channels, id=839436711367933982)
                            await message.add_reaction(u"\U0001F5C4")
                            await log_channel.send(embed=embed)

            elif (message.channel.id == 628318833953734676 or channel.name == '🔵pvp-build-grp') and \
                (y[0].startswith('<:house_nova:') or y[0].startswith('<:inhouse_nova:')) and \
                    Pending_role not in user.roles:
                if not y[3].strip():
                    embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                            color=0x5d4991)
                    embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                    embed_dm.set_footer(text=f"Timestamp: {now}")
                    
                    await user.send(
                        f"Hi **{user.name}**, your input for **__boost pot__** is either incomplete or you might have some error on "
                        "it, please double check the pot. If you are sure you "
                        "didn't do anything wrong, please contact Nova Team. Thank you!", embed=embed_dm)
                    await message.remove_reaction(u"\u2705", user)
                else:
                    pot = convert_si_to_number(y[3].partition(":")[2].replace(",", "."))
                    paid_in = y[4].partition(":")[2].strip()
                    if pot < 999:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, pot cannot be below 1K gold", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif not paid_in or not y[9].strip():
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, your input is missing **__Payment Realm and/or Booster__**, please double check."
                            "If you are sure you didn't do anything wrong, please contact Nova Team. Thank you!", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif paid_in not in realm_name:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, **__`{paid_in}`__** you used is either incomplete or you might have "
                            "some error in it, please double check. If you are sure it's not wrong, please contact "
                            "Nova Team. Thank you!", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    else:
                        adv = message.author
                        adv_name, adv_realm = await checkPers(adv.id)
                        if adv_name is None:
                            adv_result = search_nested_alliance(boosters, adv.nick)
                            if adv_result is not None:
                                adv_name, adv_realm = adv_result.split("-")
                            else:
                                adv_name, adv_realm = adv.nick.split("-")

                        tank_id_pre = y[9].partition(":")[2].partition("@")[2]
                        if tank_id_pre.startswith("!"):
                            tank_id_pre = tank_id_pre.partition("!")[2]
                        tank_id = int(tank_id_pre.partition(">")[0])
                        tank_user = get(guild.members, id=tank_id)
                        tank_nick = tank_user.nick
                        tank_name, tank_realm = await checkPers(tank_id)
                        if tank_name is None:
                            tank_result = search_nested_alliance(boosters, tank_nick)
                            if tank_result is not None:
                                tank_name, tank_realm = tank_result.split("-")
                            else:
                                tank_name, tank_realm = tank_nick.split("-")
                        #########################################################
                        if y[0].startswith('<:house_nova:'):
                            adv_cut = int(pot * 0.10)
                        elif y[0].startswith('<:inhouse_nova:'):
                            adv_cut = int(pot * 0.07)
                        booster_cut = int(pot * 0.7)

                        async with conn.cursor() as cursor:
                            query = """INSERT INTO various (boost_faction, boost_id, boost_date, boost_pot, boost_realm,
                                    adv_name, adv_realm, adv_cut, tank_name, tank_realm, tank_cut) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            val = ("Alliance", payload.message_id, now, pot, paid_in, adv_name, adv_realm, adv_cut, tank_name,
                                tank_realm, booster_cut)
                            await cursor.execute(query, val)

                            embed = discord.Embed(title="This run was successfully added to DB.", description="",
                                                color=0x5d4991)
                            embed.add_field(name="**Server**", value=paid_in, inline=True)
                            embed.add_field(name="**POT**",
                                            value=y[3].partition(":")[2].replace(",", ".") + "<:goldss:817570131193888828>", inline=True)
                            embed.add_field(name="**Advertiser**", value=adv_name +
                                            "-" + adv_realm, inline=False)
                            embed.add_field(name="**Advertiser Cut:**",
                                            value=str(adv_cut), inline=True)
                            embed.add_field(name="**Boosters Cut:**",
                                            value=str(booster_cut), inline=True)
                            embed.add_field(name="**Boosters**<:alliance_nova:817570759194968064>",
                                            value="<:tank_nova:817571065207324703>" + tank_name, inline=False)
                            embed.set_footer(text=f"{now} Run id: {payload.message_id}",
                                            icon_url="https://cdn.discordapp.com/avatars/634917649335320586"
                                            "/e950645a963e4b34502c141b90c2463f.png")
                            log_channel = get(guild.text_channels, id=839436711367933982)
                            await message.add_reaction(u"\U0001F5C4")
                            await log_channel.send(embed=embed)

            elif channel.name.startswith('🔵rbg-run-submit') and \
                (y[1].startswith('<:house_nova:') or y[1].startswith('<:inhouse_nova:')) and \
                    Pending_role not in user.roles:
                if not y[4].strip():
                    embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                            color=0x5d4991)
                    embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                    embed_dm.set_footer(text=f"Timestamp: {now}")
                    
                    await user.send(
                        f"Hi **{user.name}**, your input for **__boost pot__** is either incomplete or you might have some error on "
                        "it, please double check the pot. If you are sure you "
                        "didn't do anything wrong, please contact Nova Team. Thank you!", embed=embed_dm)
                    await message.remove_reaction(u"\u2705", user)
                else:
                    pot = convert_si_to_number(y[4].partition(">")[2].replace(",", "."))
                    paid_in = y[3].partition(">")[2]
                    paid_in = paid_in.strip()
                    if pot < 999:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, pot cannot be below 1K gold", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif not paid_in or not y[5].strip():
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, your input is missing **__Payment Realm and/or Booster__**, please double check."
                            "If you are sure you didn't do anything wrong, please contact Nova Team. Thank you!", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif paid_in not in realm_name:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, **__`{paid_in}`__** you used is either incomplete or you might have "
                            "some error in it, please double check. If you are sure it's not wrong, please contact "
                            "Nova Team. Thank you!", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    else:
                        adv = message.author
                        adv_name, adv_realm = await checkPers(adv.id)
                        if adv_name is None:
                            adv_result = search_nested_alliance(boosters, adv.nick)
                            if adv_result is not None:
                                adv_name, adv_realm = adv_result.split("-")
                            else:
                                adv_name, adv_realm = adv.nick.split("-")
                        ###########################################################
                        tank_id_pre = y[5].partition("@")[2]
                        if tank_id_pre.startswith("!"):
                            tank_id_pre = tank_id_pre.partition("!")[2]
                        tank_id = int(tank_id_pre.partition(">")[0])
                        tank_user = get(guild.members, id=tank_id)
                        tank_nick = tank_user.nick
                        tank_name, tank_realm = await checkPers(tank_id)
                        if tank_name is None:
                            tank_result = search_nested_alliance(boosters, tank_nick)
                            if tank_result is not None:
                                tank_name, tank_realm = tank_result.split("-")
                            else:
                                tank_name, tank_realm = tank_nick.split("-")
                        #########################################################
                        ##############################################
                        if y[1].startswith('<:house_nova:'):
                            adv_cut = int(pot * 0.05)
                        elif y[1].startswith('<:inhouse_nova:'):
                            adv_cut = int(pot * 0)
                        booster_cut = int(pot * 0.8)

                        async with conn.cursor() as cursor:
                            query = """INSERT INTO various (boost_faction, boost_id, boost_date, boost_pot, boost_realm,
                                    adv_name, adv_realm, adv_cut, tank_name, tank_realm, tank_cut)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            val = ("Alliance", payload.message_id, now, pot, paid_in, adv_name, adv_realm, adv_cut, tank_name,
                                tank_realm, booster_cut)
                            await cursor.execute(query, val)

                            embed = discord.Embed(title="This run was successfully added to DB.", description="",
                                                color=0x5d4991)
                            embed.add_field(name="**Server**", value=paid_in, inline=True)
                            embed.add_field(name="**POT**",
                                            value=y[4].partition(">")[2] + "<:goldss:817570131193888828>", inline=True)
                            embed.add_field(name="**Advertiser**", value=adv_name +
                                            "-" + adv_realm, inline=False)
                            embed.add_field(name="**Advertiser Cut:**",
                                            value=str(adv_cut), inline=True)
                            embed.add_field(name="**Boosters Cut:**",
                                            value=str(booster_cut), inline=True)
                            embed.add_field(name="**Boosters**<:alliance_nova:817570759194968064>",
                                            value="<:tank_nova:817571065207324703>" + tank_name, inline=False)
                            embed.set_footer(text=f"{now} Run id: {payload.message_id}",
                                            icon_url="https://cdn.discordapp.com/avatars/634917649335320586"
                                            "/e950645a963e4b34502c141b90c2463f.png")
                            log_channel = get(guild.text_channels, id=839436711367933982)
                            await message.add_reaction(u"\U0001F5C4")
                            await log_channel.send(embed=embed)
            # endregion

            # region Horde build groups
            elif (channel.name.startswith('build-grp') or channel.name.startswith('high-keys-grp')) and \
                (y[1].startswith('<:house_nova:') or y[1].startswith('<:inhouse_nova:')) and \
                    PendingH_role not in user.roles:
                if not y[4].strip():
                    embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                            color=0x5d4991)
                    embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                    embed_dm.set_footer(text=f"Timestamp: {now}")
                    
                    await user.send(
                        f"Hi **{user.name}**, your input for **__boost pot__** is either incomplete or you might have some error on "
                        "it, please double check the pot. If you are sure you "
                        "didn't do anything wrong, please contact Nova Team. Thank you!", embed=embed_dm)
                    await message.remove_reaction(u"\u2705", user)
                else:
                    pot = convert_si_to_number(y[4].partition(">")[2].replace(",", "."))
                    paid_in = y[3].partition(">")[2].strip()
                    if pot < 999:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, pot cannot be below 1K gold", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif not paid_in or not y[5].strip():
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, your input is missing **__Payment Realm and/or Booster__**, please double check."
                            "If you are sure you didn't do anything wrong, please contact Nova Team. Thank you!", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif paid_in not in realm_name:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, **__`{paid_in}`__** you used is either incomplete or you might have "
                            "some error in it, please double check. If you are sure it's not wrong, please contact "
                            "Nova Team. Thank you!", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    else:
                        adv = message.author
                        adv_name, adv_realm = await checkPers(adv.id)
                        if adv_name is None:
                            adv_result = search_nested_horde(boosters, adv.nick)
                            if adv_result is not None:
                                adv_name, adv_realm = adv_result.split("-")
                            else:
                                adv_name, adv_realm = adv.nick.split("-")

                        tank_id_pre = y[5].partition("@")[2]
                        if tank_id_pre.startswith("!"):
                            tank_id_pre = tank_id_pre.partition("!")[2]
                        tank_id = int(tank_id_pre.partition(">")[0])
                        tank_user = get(guild.members, id=tank_id)
                        tank_nick = tank_user.nick
                        tank_name, tank_realm = await checkPers(tank_id)
                        if tank_name is None:
                            tank_result = search_nested_horde(boosters, tank_nick)
                            if tank_result is not None:
                                tank_name, tank_realm = tank_result.split("-")
                            else:
                                tank_name, tank_realm = tank_nick.split("-")

                        healer_id_pre = y[6].partition("@")[2]
                        if healer_id_pre.startswith("!"):
                            healer_id_pre = healer_id_pre.partition("!")[2]
                        healer_id = int(healer_id_pre.partition(">")[0])
                        healer_user = get(guild.members, id=healer_id)
                        healer_nick = healer_user.nick
                        healer_name, healer_realm = await checkPers(healer_id)
                        if healer_name is None:
                            healer_result = search_nested_horde(boosters, healer_nick)
                            if healer_result is not None:
                                healer_name, healer_realm = healer_result.split("-")
                            else:
                                healer_name, healer_realm = healer_nick.split("-")
                                
                        dps1_id_pre = y[7].partition("@")[2]
                        if dps1_id_pre.startswith("!"):
                            dps1_id_pre = dps1_id_pre.partition("!")[2]
                        dps1_id = int(dps1_id_pre.partition(">")[0])
                        dps1_user = get(guild.members, id=dps1_id)
                        dps1_nick = dps1_user.nick
                        dps1_name, dps1_realm = await checkPers(dps1_id)
                        if dps1_name is None:
                            dps1_result = search_nested_horde(boosters, dps1_nick)
                            if dps1_result is not None:
                                dps1_name, dps1_realm = dps1_result.split("-")
                            else:
                                dps1_name, dps1_realm = dps1_nick.split("-")

                        dps2_id_pre = y[8].partition("@")[2]
                        if dps2_id_pre.startswith("!"):
                            dps2_id_pre = dps2_id_pre.partition("!")[2]
                        dps2_id = int(dps2_id_pre.partition(">")[0])
                        dps2_user = get(guild.members, id=dps2_id)
                        dps2_nick = dps2_user.nick
                        dps2_name, dps2_realm = await checkPers(dps2_id)
                        if dps2_name is None:
                            dps2_result = search_nested_horde(boosters, dps2_nick)
                            if dps2_result is not None:
                                dps2_name, dps2_realm = dps2_result.split("-")
                            else:
                                dps2_name, dps2_realm = dps2_nick.split("-")
                            
                        ##############################################
                        if y[1].startswith('<:house_nova:'):
                            adv_cut = int(pot * 0.10)
                        elif y[1].startswith('<:inhouse_nova:'):
                            adv_cut = int(pot * 0.07)
                        booster_cut = int(pot * 0.175)

                        async with conn.cursor() as cursor:
                            query = """INSERT INTO m_plus (boost_faction, boost_id, boost_date, boost_pot, boost_realm,
                                    adv_name, adv_realm, adv_cut, tank_name, tank_realm, tank_cut,
                                    healer_name, healer_realm, healer_cut, dps1_name, dps1_realm, dps1_cut,
                                    dps2_name, dps2_realm, dps2_cut) VALUES (%s, %s, %s, %s, %s, %s, 
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            val = ("Horde", payload.message_id, now, pot, paid_in, adv_name, adv_realm, adv_cut, tank_name,
                                tank_realm, booster_cut, healer_name, healer_realm, booster_cut, dps1_name,
                                dps1_realm, booster_cut, dps2_name, dps2_realm, booster_cut)
                            await cursor.execute(query, val)

                            embed = discord.Embed(title="This run was successfully added to DB.", description="",
                                                color=0x5d4991)
                            embed.add_field(name="**Server**", value=paid_in, inline=True)
                            embed.add_field(name="**POT**",
                                            value=y[4].partition(">")[2] + "<:goldss:817570131193888828>", inline=True)
                            embed.add_field(name="**Advertiser**", value=adv_name +
                                            "-" + adv_realm, inline=False)
                            embed.add_field(name="**Advertiser Cut:**",
                                            value=str(adv_cut), inline=True)
                            embed.add_field(name="**Boosters Cut:**",
                                            value=str(booster_cut), inline=True)
                            embed.add_field(name="**Boosters**<:horde_nova:817556558435188747>",
                                            value="<:tank_nova:817571065207324703>" + tank_name + " <:healer_nova:817571133066838016>" + healer_name + " <:dps_nova:817571146907385876>" + dps1_name + " <:dps_nova:817571146907385876>" + dps2_name, inline=False)
                            embed.set_footer(text=f"{now} Run id: {payload.message_id}",
                                            icon_url="https://cdn.discordapp.com/avatars/634917649335320586"
                                            "/e950645a963e4b34502c141b90c2463f.png")
                            log_channel = get(guild.text_channels, id=839436711367933982)
                            await message.add_reaction(u"\U0001F5C4")
                            await log_channel.send(embed=embed)

            elif channel.name == '🔴leveling-torghast-boost' and \
                (y[1].startswith('<:house_nova:') or y[1].startswith('<:inhouse_nova:')) and \
                    PendingH_role not in user.roles:
                if not y[4].strip():
                    embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                            color=0x5d4991)
                    embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                    embed_dm.set_footer(text=f"Timestamp: {now}")
                    
                    await user.send(
                        f"Hi **{user.name}**, your input for **__boost pot__** is either incomplete or you might have some error on "
                        "it, please double check the pot. If you are sure you "
                        "didn't do anything wrong, please contact Nova Team. Thank you!", embed=embed_dm)
                    await message.remove_reaction(u"\u2705", user)
                else:
                    pot = convert_si_to_number(y[4].partition(">")[2].replace(",", "."))
                    paid_in = y[3].partition(">")[2].strip()
                    if pot < 999:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, pot cannot be below 1K gold", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif not paid_in or not y[5].strip():
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, your input is missing **__Payment Realm and/or Booster__**, please double check."
                            "If you are sure you didn't do anything wrong, please contact Nova Team. Thank you!", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif paid_in not in realm_name:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, **__`{paid_in}`__** you used is either incomplete or you might have "
                            "some error in it, please double check. If you are sure it's not wrong, please contact "
                            "Nova Team. Thank you!", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    else:
                        adv = message.author
                        adv_name, adv_realm = await checkPers(adv.id)
                        if adv_name is None:
                            adv_result = search_nested_horde(boosters, adv.nick)
                            if adv_result is not None:
                                adv_name, adv_realm = adv_result.split("-")
                            else:
                                adv_name, adv_realm = adv.nick.split("-")
                        ###########################################################
                        tank_id_pre = y[5].partition("@")[2]
                        if tank_id_pre.startswith("!"):
                            tank_id_pre = tank_id_pre.partition("!")[2]
                        tank_id = int(tank_id_pre.partition(">")[0])
                        tank_user = get(guild.members, id=tank_id)
                        tank_nick = tank_user.nick
                        tank_name, tank_realm = await checkPers(tank_id)
                        if tank_name is None:
                            tank_result = search_nested_horde(boosters, tank_nick)
                            if tank_result is not None:
                                tank_name, tank_realm = tank_result.split("-")
                            else:
                                tank_name, tank_realm = tank_nick.split("-")
                        #########################################################

                        if y[1].startswith('<:house_nova:'):
                            adv_cut = int(pot * 0.10)
                        elif y[1].startswith('<:inhouse_nova:'):
                            adv_cut = int(pot * 0.07)
                        booster_cut = int(pot * 0.7)

                        async with conn.cursor() as cursor:
                            query = """INSERT INTO various (boost_faction, boost_id, boost_date, boost_pot, boost_realm,
                                    adv_name, adv_realm, adv_cut, tank_name, tank_realm, tank_cut)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            val = ("Horde", payload.message_id, now, pot, paid_in, adv_name, adv_realm, adv_cut, tank_name,
                                tank_realm, booster_cut)
                            await cursor.execute(query, val)

                            embed = discord.Embed(title="This run was successfully added to DB.", description="",
                                                color=0x5d4991)
                            embed.add_field(name="**Server**", value=paid_in, inline=True)
                            embed.add_field(name="**POT**",
                                            value=y[4].partition(">")[2] + "<:goldss:817570131193888828>", inline=True)
                            embed.add_field(name="**Advertiser**", value=adv_name +
                                            "-" + adv_realm, inline=False)
                            embed.add_field(name="**Advertiser Cut:**",
                                            value=str(adv_cut), inline=True)
                            embed.add_field(name="**Boosters Cut:**",
                                            value=str(booster_cut), inline=True)
                            embed.add_field(name="**Boosters**<:alliance_nova:817570759194968064>",
                                            value="<:tank_nova:817571065207324703>" + tank_name, inline=False)
                            embed.set_footer(text=f"{now} Run id: {payload.message_id}",
                                            icon_url="https://cdn.discordapp.com/avatars/634917649335320586"
                                            "/e950645a963e4b34502c141b90c2463f.png")
                            log_channel = get(guild.text_channels, id=839436711367933982)
                            await message.add_reaction(u"\U0001F5C4")
                            await log_channel.send(embed=embed)

            elif (message.channel.id == 714121043924484146 or channel.name == '🔴pvp-build-grp') and \
                (y[0].startswith('<:house_nova:') or y[0].startswith('<:inhouse_nova:')) and \
                    PendingH_role not in user.roles:
                if not y[3].strip():
                    embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                            color=0x5d4991)
                    embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                    embed_dm.set_footer(text=f"Timestamp: {now}")
                    
                    await user.send(
                        f"Hi **{user.name}**, your input for **__boost pot__** is either incomplete or you might have some error on "
                        "it, please double check the pot. If you are sure you "
                        "didn't do anything wrong, please contact Nova Team. Thank you!", embed=embed_dm)
                    await message.remove_reaction(u"\u2705", user)
                else:
                    pot = convert_si_to_number(y[3].partition(":")[2].replace(",", "."))
                    paid_in = y[4].partition(":")[2].strip()
                    if pot < 999:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, pot cannot be below 1K gold", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif not paid_in or not y[9].strip():
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, your input is missing **__Payment Realm and/or Booster__**, please double check."
                            "If you are sure you didn't do anything wrong, please contact Nova Team. Thank you!", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif paid_in not in realm_name:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, **__`{paid_in}`__** you used is either incomplete or you might have "
                            "some error in it, please double check. If you are sure it's not wrong, please contact "
                            "Nova Team. Thank you!", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    else:
                        adv = message.author
                        adv_name, adv_realm = await checkPers(adv.id)
                        if adv_name is None:
                            adv_result = search_nested_horde(boosters, adv.nick)
                            if adv_result is not None:
                                adv_name, adv_realm = adv_result.split("-")
                            else:
                                adv_name, adv_realm = adv.nick.split("-")

                        tank_id_pre = y[9].partition(":")[2].partition("@!")[2]
                        tank_id = int(tank_id_pre.partition(">")[0])
                        tank_user = get(guild.members, id=tank_id)
                        tank_nick = tank_user.nick
                        tank_name, tank_realm = await checkPers(tank_id)
                        if tank_name is None:
                            tank_result = search_nested_horde(boosters, tank_nick)
                            if tank_result is not None:
                                tank_name, tank_realm = tank_result.split("-")
                            else:
                                tank_name, tank_realm = tank_nick.split("-")
                        #########################################################

                        if y[0].startswith('<:house_nova:'):
                            adv_cut = int(pot * 0.10)
                        elif y[0].startswith('<:inhouse_nova:'):
                            adv_cut = int(pot * 0.07)
                        booster_cut = int(pot * 0.7)

                        async with conn.cursor() as cursor:
                            query = """INSERT INTO various (boost_faction, boost_id, boost_date, boost_pot, boost_realm,
                                    adv_name, adv_realm, adv_cut, tank_name, tank_realm, tank_cut)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            val = ("Horde", payload.message_id, now, pot, paid_in, adv_name, adv_realm, adv_cut, tank_name,
                                tank_realm, booster_cut)
                            await cursor.execute(query, val)

                            embed = discord.Embed(title="This run was successfully added to DB.", description="",
                                                color=0x5d4991)
                            embed.add_field(name="**Server**", value=paid_in, inline=True)
                            embed.add_field(name="**POT**",
                                            value=y[3].partition(":")[2].replace(",", ".") + "<:goldss:817570131193888828>", inline=True)
                            embed.add_field(name="**Advertiser**", value=adv_name +
                                            "-" + adv_realm, inline=False)
                            embed.add_field(name="**Advertiser Cut:**",
                                            value=str(adv_cut), inline=True)
                            embed.add_field(name="**Boosters Cut:**",
                                            value=str(booster_cut), inline=True)
                            embed.add_field(name="**Boosters**<:horde_nova:817556558435188747>",
                                            value="<:tank_nova:817571065207324703>" + tank_name, inline=False)
                            embed.set_footer(text=f"{now} Run id: {payload.message_id}",
                                            icon_url="https://cdn.discordapp.com/avatars/634917649335320586"
                                            "/e950645a963e4b34502c141b90c2463f.png")
                            log_channel = get(guild.text_channels, id=839436711367933982)
                            await message.add_reaction(u"\U0001F5C4")
                            await log_channel.send(embed=embed)

            elif channel.name.startswith('🔴rbg-run-submit') and \
                (y[1].startswith('<:house_nova:') or y[1].startswith('<:inhouse_nova:')) and \
                    PendingH_role not in user.roles:
                if not y[4].strip():
                    embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                            color=0x5d4991)
                    embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                    embed_dm.set_footer(text=f"Timestamp: {now}")
                    
                    await user.send(
                        f"Hi **{user.name}**, your input for **__boost pot__** is either incomplete or you might have some error on "
                        "it, please double check the pot. If you are sure you "
                        "didn't do anything wrong, please contact Nova Team. Thank you!", embed=embed_dm)
                    await message.remove_reaction(u"\u2705", user)
                else:
                    pot = convert_si_to_number(y[4].partition(">")[2].replace(",", "."))
                    paid_in = y[3].partition(">")[2]
                    paid_in = paid_in.strip()
                    if pot < 999:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, pot cannot be below 1K gold", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif not paid_in or not y[5].strip():
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, your input is missing **__Payment Realm and/or Booster__**, please double check."
                            "If you are sure you didn't do anything wrong, please contact Nova Team. Thank you!", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    elif paid_in not in realm_name:
                        embed_dm = discord.Embed(title="Refer to this run.", description=message.content,
                                                color=0x5d4991)
                        embed_dm.add_field(name="Link", value=message.jump_url, inline=True)
                        embed_dm.set_footer(text=f"Timestamp: {now}")
                        
                        await user.send(
                            f"Hi **{user.name}**, **__`{paid_in}`__** you used is either incomplete or you might have "
                            "some error in it, please double check. If you are sure it's not wrong, please contact "
                            "Nova Team. Thank you!", embed=embed_dm)
                        await message.remove_reaction(u"\u2705", user)
                    else:
                        adv = message.author
                        adv_name, adv_realm = await checkPers(adv.id)
                        if adv_name is None:
                            adv_result = search_nested_horde(boosters, adv.nick)
                            if adv_result is not None:
                                adv_name, adv_realm = adv_result.split("-")
                            else:
                                adv_name, adv_realm = adv.nick.split("-")
                        ###########################################################
                        tank_id_pre = y[5].partition("@")[2]
                        if tank_id_pre.startswith("!"):
                            tank_id_pre = tank_id_pre.partition("!")[2]
                        tank_id = int(tank_id_pre.partition(">")[0])
                        tank_user = get(guild.members, id=tank_id)
                        tank_nick = tank_user.nick
                        tank_name, tank_realm = await checkPers(tank_id)
                        if tank_name is None:
                            tank_result = search_nested_horde(boosters, tank_nick)
                            if tank_result is not None:
                                tank_name, tank_realm = tank_result.split("-")
                            else:
                                tank_name, tank_realm = tank_nick.split("-")
                        #########################################################
                        ##############################################
                        if y[1].startswith('<:house_nova:'):
                            adv_cut = int(pot * 0.05)
                        elif y[1].startswith('<:inhouse_nova:'):
                            adv_cut = int(pot * 0.00)
                        booster_cut = int(pot * 0.8)

                        async with conn.cursor() as cursor:
                            query = """INSERT INTO various (boost_faction, boost_id, boost_date, boost_pot, boost_realm,
                                    adv_name, adv_realm, adv_cut, tank_name, tank_realm, tank_cut)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            val = ("Horde", payload.message_id, now, pot, paid_in, adv_name, adv_realm, adv_cut, tank_name,
                                tank_realm, booster_cut)
                            await cursor.execute(query, val)

                            embed = discord.Embed(title="This run was successfully added to DB.", description="",
                                                color=0x5d4991)
                            embed.add_field(name="**Server**", value=paid_in, inline=True)
                            embed.add_field(name="**POT**",
                                            value=y[4].partition(">")[2] + "<:goldss:817570131193888828>", inline=True)
                            embed.add_field(name="**Advertiser**", value=adv_name +
                                            "-" + adv_realm, inline=False)
                            embed.add_field(name="**Advertiser Cut:**",
                                            value=str(adv_cut), inline=True)
                            embed.add_field(name="**Boosters Cut:**",
                                            value=str(booster_cut), inline=True)
                            embed.add_field(name="**Boosters**<:horde_nova:817556558435188747>",
                                            value="<:tank_nova:817571065207324703>" + tank_name, inline=False)
                            embed.set_footer(text=f"{now} Run id: {payload.message_id}",
                                            icon_url="https://cdn.discordapp.com/avatars/634917649335320586"
                                            "/e950645a963e4b34502c141b90c2463f.png")
                            log_channel = get(guild.text_channels, id=839436711367933982)
                            await message.add_reaction(u"\U0001F5C4")
                            await log_channel.send(embed=embed)
                # endregion


@bot.event
async def on_message(message):
    if (message.author.id == troll_target and (not message.channel.name.startswith('high-') or 
        not message.channel.name.startswith('build-'))):
        await message.delete()
    
    elif (message.author.id == troll_target1 and (not message.channel.name.startswith('high-') or 
        not message.channel.name.startswith('build-'))):
        goofed_user = get(message.guild.members, id=troll_target1)
        await message.channel.send(f"{goofed_user.mention} <:TopKeK:834820771335110736>")
    
    elif (message.author.id == troll_target2 and (not message.channel.name.startswith('high-') or 
        not message.channel.name.startswith('build-'))):
        goofed_user2 = get(message.guild.members, id=troll_target2)
        await message.channel.send(f"{goofed_user2.mention} <:pepeUwU:817580605436461106>")
    
    elif (message.author.id == troll_target3 and (not message.channel.name.startswith('high-') or 
        not message.channel.name.startswith('build-'))):
        goofed_user3 = get(message.guild.members, id=troll_target3)
        await goofed_user3.create_dm()
        await goofed_user3.dm_channel.send("""
            Some people may call this a spam, but what is spam?
            merriam-webster defines it as irrelevant or unsolicited messages sent over the Internet, typically to a 
            large number of users, for the purposes of advertising, phishing, spreading malware, etc.
            But is it really a spam? what if the message is actually trying to educate you of what spam is, 
            by the way, do you know The term spam is derived from the 1970 'Spam' sketch of the BBC television comedy 
            series Monty Python's Flying Circus.The sketch, set in a cafe, has a waitress reading out a menu where 
            every item but one includes Spam canned luncheon meat. As the waitress recites the Spam-filled menu, 
            a chorus of Viking patrons drown out all conversations with a song, repeating 'Spam, Spam, Spam, Spam… 
            Lovely Spam! Wonderful Spam!'
            The excessive amount of Spam mentioned is a reference to the ubiquity of it and other imported canned 
            meat products in the UK after World War II (a period of rationing in the UK) as the country struggled 
            to rebuild its agricultural base.
            """
        )
    
    elif isinstance(message.channel, discord.DMChannel):
        if not message.author.bot:
            guild = get(bot.guilds, id=GUILD_ID)
            bot_dms__channel = get(guild.text_channels, name='bot-dms-test')
            await bot_dms__channel.send(f"{message.created_at} -- {message.author.name} sent -- `{message.content}`")
            if len(message.attachments) >= 1:
                await bot_dms__channel.send(message.attachments)
                await bot_dms__channel.send(message.attachments[0].url)
    
    elif ((message.channel.id == 815104636251275312 or message.channel.name == "balance-check") and 
        (not message.content.lower() == "!b" and  not message.content.lower() == "!b_crossfaction") and 
        not message.author.bot and (get(message.guild.roles, name="NOVA") not in message.author.roles or 
        get(message.guild.roles, name="Moderator") not in message.author.roles)):
        await message.channel.send("Wrong balance check command", delete_after=3)
        await message.delete()
    
    else:
        x = message.content.split("\n")
        AdvertiserA_role = get(message.guild.roles, name="Advertiser {A}")
        AdvertiserA_trial_role = get(message.guild.roles, name="Trial Advertiser {A}")
        AdvertiserH_role = get(message.guild.roles, name="Advertiser {H}")
        AdvertiserH_trial_role = get(message.guild.roles, name="Trial Advertiser {H}")
        Pending_role = get(message.guild.roles, name='Pending')
        PendingH_role = get(message.guild.roles, name='Pending [H]')
        Staff_role = get(message.guild.roles, id=815104630538895451)
        CommunitySupport_role = get(message.guild.roles, name="Community Support")
        Management_role = get(message.guild.roles, name="Management")
        Nova_role = get(message.guild.roles, name="NOVA")
        Moderator_role = get(message.guild.roles, name="Moderator")
        TeamLeader_role = get(message.guild.roles, name="Team Leader")
        roles_to_check = [AdvertiserA_role, AdvertiserA_trial_role, 
                AdvertiserH_role, AdvertiserH_trial_role, Staff_role, 
                Management_role, Nova_role, Moderator_role, CommunitySupport_role]
        roles_check =  any(item in message.author.roles for item in roles_to_check)

        if message.channel.name.startswith("post-run") and message.author.bot:
                await message.add_reaction(u"\U0001F4B0")
                if len(message.reactions) <= 0:
                    await message.add_reaction(u"\U0001F4B0")
        
        if message.channel.name == "collectors" and message.author.bot:
            await message.add_reaction(u"\u2705")

        if len(x) > 1:
            if (x[1].startswith('<:keystone_nova:') or x[1].startswith(u"\U0001F53C") 
                or x[1].startswith('<:inhouse_nova:') or x[1].startswith('<:house_nova:')):
                if (message.channel.name.startswith('build-group') or message.channel.name.startswith('high-keys-group') or 
                    (message.channel.id == 815104637391863857 or message.channel.name == "🔵leveling-torghast-boost") or 
                    (message.channel.id == 815104639368298545 or message.channel.name == "🔵rbg-run-submit")):
                    if roles_check and Pending_role not in message.author.roles:
                        await message.channel.send("3", delete_after=1)
                        await asyncio.sleep(1)
                        await message.channel.send("2", delete_after=1)
                        await asyncio.sleep(1)
                        await message.channel.send("1", delete_after=1)
                        await asyncio.sleep(1)
                        await message.channel.send("`Ignore sign ups above this message those are pretypers and they should feel bad`")
                        await message.add_reaction(u"\u2705")
                        if (AdvertiserA_trial_role in message.author.roles and 
                            (x[1].startswith('<:inhouse_nova:') or x[1].startswith('<:house_nova:'))):
                            collectors_channel = get(message.guild.text_channels, name='collectors')
                            collectors_role = get(message.guild.roles, name="Collectors")
                            collectors_tag_msg = await collectors_channel.send(collectors_role.mention)
                            embed_collection_log = discord.Embed(
                                title="Gold Collection", description="Run has been posted", color=0x5d4991)
                            embed_collection_log.add_field(
                                name="Author", value=message.author.mention, inline=True)
                            embed_collection_log.add_field(name="Realm: ", value=x[3], inline=True)
                            embed_collection_log.add_field(name="Amount: ", value=x[4], inline=True)
                            embed_collection_log.add_field(
                                name="Channel", value=message.channel.name, inline=False)
                            embed_collection_log.add_field(
                                name="Link", value=message.jump_url, inline=True)
                            embed_collection_log.set_footer(text=datetime.now(timezone.utc).replace(microsecond=0))
                            await collectors_channel.send(embed=embed_collection_log)
                            await collectors_tag_msg.clear_reactions()
                            await collectors_tag_msg.clear_reactions()
                            await message.clear_reactions()
                            await message.add_reaction(u"\U0001F513")
                            await message.author.add_roles(Pending_role)
                        else:
                            collectors_channel = get(message.guild.text_channels, name='collectors')
                            collectors_role = get(message.guild.roles, name="Collectors")
                            collectors_tag_msg = await collectors_channel.send(collectors_role.mention)
                            embed_collection_log = discord.Embed(
                                title="Gold Collection", description="Run has been posted", color=0x5d4991)
                            embed_collection_log.add_field(
                                name="Author", value=message.author.mention, inline=True)
                            embed_collection_log.add_field(name="Realm: ", value=x[2], inline=True)
                            embed_collection_log.add_field(name="Amount: ", value=x[3], inline=True)
                            embed_collection_log.add_field(
                                name="Channel", value=message.channel.name, inline=False)
                            embed_collection_log.add_field(
                                name="Link", value=message.jump_url, inline=True)
                            embed_collection_log.set_footer(text=datetime.now(timezone.utc).replace(microsecond=0))
                            await collectors_channel.send(embed=embed_collection_log)
                            await collectors_tag_msg.clear_reactions()
                            await collectors_tag_msg.clear_reactions()
                            await message.clear_reactions()
                            await message.add_reaction(u"\U0001F513")
                            await message.author.add_roles(Pending_role)
                    else:
                        await message.delete()
                elif (message.channel.name.startswith('build-grp') or message.channel.name.startswith('high-keys-grp') or
                    (message.channel.id == 815104637697916959 or message.channel.name == "🔴leveling-torghast-boost") or 
                    (message.channel.id == 815104639661375488 or message.channel.name == "🔴rbg-run-submit")):
                    if roles_check and PendingH_role not in message.author.roles:
                        await message.channel.send("3", delete_after=1)
                        await asyncio.sleep(1)
                        await message.channel.send("2", delete_after=1)
                        await asyncio.sleep(1)
                        await message.channel.send("1", delete_after=1)
                        await asyncio.sleep(1)
                        await message.channel.send("`Ignore sign ups above this message those are pretypers and they should feel bad`")
                        await message.add_reaction(u"\u2705")
                        if (AdvertiserH_trial_role in message.author.roles and 
                            (x[1].startswith('<:inhouse_nova:') or x[1].startswith('<:house_nova:'))):
                            collectors_channel = get(message.guild.text_channels, name='collectors')
                            collectors_role = get(message.guild.roles, name="Collectors")
                            collectors_tag_msg = await collectors_channel.send(collectors_role.mention)
                            embed_collection_log = discord.Embed(
                                title="Gold Collection", description="Run has been posted", color=0x5d4991)
                            embed_collection_log.add_field(
                                name="Author", value=message.author.mention, inline=True)
                            embed_collection_log.add_field(name="Realm: ", value=x[3], inline=True)
                            embed_collection_log.add_field(name="Amount: ", value=x[4], inline=True)
                            embed_collection_log.add_field(
                                name="Channel", value=message.channel.name, inline=False)
                            embed_collection_log.add_field(
                                name="Link", value=message.jump_url, inline=True)
                            embed_collection_log.set_footer(text=datetime.now(timezone.utc).replace(microsecond=0))
                            await collectors_channel.send(embed=embed_collection_log)
                            await collectors_tag_msg.clear_reactions()
                            await collectors_tag_msg.clear_reactions()
                            await message.clear_reactions()
                            await message.add_reaction(u"\U0001F513")
                            await message.author.add_roles(PendingH_role)
                        else:
                            collectors_channel = get(message.guild.text_channels, name='collectors')
                            collectors_role = get(message.guild.roles, name="Collectors")
                            collectors_tag_msg = await collectors_channel.send(collectors_role.mention)
                            embed_collection_log = discord.Embed(
                                title="Gold Collection", description="Run has been posted", color=0x5d4991)
                            embed_collection_log.add_field(
                                name="Author", value=message.author.mention, inline=True)
                            embed_collection_log.add_field(name="Realm: ", value=x[2], inline=True)
                            embed_collection_log.add_field(name="Amount: ", value=x[3], inline=True)
                            embed_collection_log.add_field(
                                name="Channel", value=message.channel.name, inline=False)
                            embed_collection_log.add_field(
                                name="Link", value=message.jump_url, inline=True)
                            embed_collection_log.set_footer(text=datetime.now(timezone.utc).replace(microsecond=0))
                            await collectors_channel.send(embed=embed_collection_log)
                            await collectors_tag_msg.clear_reactions()
                            await collectors_tag_msg.clear_reactions()
                            await message.clear_reactions()
                            await message.add_reaction(u"\U0001F513")
                            await message.author.add_roles(PendingH_role)
                    else:
                        await message.delete()
            
            elif ((x[0].startswith('Type of Boost:') or x[0].startswith('<:inhouse_nova:') or x[0].startswith('<:house_nova:')) and 
                    (message.channel.id == 815104639082823699 or message.channel.name == "🔵pvp-build-grp")):
                if roles_check and Pending_role not in message.author.roles:
                    await message.add_reaction(u"\u2705")
                    if (AdvertiserA_trial_role in message.author.roles and 
                        (x[0].startswith('<:inhouse_nova:') or x[0].startswith('<:house_nova:'))):
                        collectors_channel = get(message.guild.text_channels, name='collectors')
                        collectors_role = get(message.guild.roles, name="Collectors")
                        collectors_tag_msg = await collectors_channel.send(collectors_role.mention)
                        embed_collection_log = discord.Embed(
                            title="Gold Collection", description="Run has been posted", color=0x5d4991)
                        embed_collection_log.add_field(
                            name="Author", value=message.author.mention, inline=True)
                        embed_collection_log.add_field(name="Realm: ", value=x[4], inline=True)
                        embed_collection_log.add_field(name="Amount: ", value=x[3], inline=True)
                        embed_collection_log.add_field(
                            name="Channel", value=message.channel.name, inline=False)
                        embed_collection_log.add_field(
                            name="Link", value=message.jump_url, inline=True)
                        embed_collection_log.set_footer(text=datetime.now(timezone.utc).replace(microsecond=0))
                        await collectors_channel.send(embed=embed_collection_log)
                        await collectors_tag_msg.clear_reactions()
                        await collectors_tag_msg.clear_reactions()
                        await message.clear_reactions()
                        await message.add_reaction(u"\U0001F513")
                        await message.author.add_roles(Pending_role)
                    else:
                        collectors_channel = get(message.guild.text_channels, name='collectors')
                        collectors_role = get(message.guild.roles, name="Collectors")
                        collectors_tag_msg = await collectors_channel.send(collectors_role.mention)
                        embed_collection_log = discord.Embed(
                            title="Gold Collection", description="Run has been posted", color=0x5d4991)
                        embed_collection_log.add_field(
                            name="Author", value=message.author.mention, inline=True)
                        embed_collection_log.add_field(name="Realm: ", value=x[3], inline=True)
                        embed_collection_log.add_field(name="Amount: ", value=x[2], inline=True)
                        embed_collection_log.add_field(
                            name="Channel", value=message.channel.name, inline=False)
                        embed_collection_log.add_field(
                            name="Link", value=message.jump_url, inline=True)
                        embed_collection_log.set_footer(text=datetime.now(timezone.utc).replace(microsecond=0))
                        await collectors_channel.send(embed=embed_collection_log)
                        await collectors_tag_msg.clear_reactions()
                        await message.clear_reactions()
                        await message.add_reaction(u"\U0001F513")
                        await message.author.add_roles(Pending_role)
                else:
                    await message.delete()

            elif ((x[0].startswith('Type of Boost:') or x[0].startswith('<:inhouse_nova:') or x[0].startswith('<:house_nova:')) and 
                (message.channel.id == 815104639368298536 or message.channel.name == "🔴pvp-build-grp")):
                if roles_check and PendingH_role not in message.author.roles:
                    await message.add_reaction(u"\u2705")
                    if (AdvertiserH_trial_role in message.author.roles and 
                        (x[0].startswith('<:inhouse_nova:') or x[0].startswith('<:house_nova:'))):
                        collectors_channel = get(message.guild.text_channels, name='collectors')
                        collectors_role = get(message.guild.roles, name="Collectors")
                        collectors_tag_msg = await collectors_channel.send(collectors_role.mention)
                        embed_collection_log = discord.Embed(
                            title="Gold Collection", description="Run has been posted", color=0x5d4991)
                        embed_collection_log.add_field(
                            name="Author", value=message.author.mention, inline=True)
                        embed_collection_log.add_field(name="Realm: ", value=x[4], inline=True)
                        embed_collection_log.add_field(name="Amount: ", value=x[3], inline=True)
                        embed_collection_log.add_field(
                            name="Channel", value=message.channel.name, inline=False)
                        embed_collection_log.add_field(
                            name="Link", value=message.jump_url, inline=True)
                        embed_collection_log.set_footer(text=datetime.now(timezone.utc).replace(microsecond=0))
                        await collectors_channel.send(embed=embed_collection_log)
                        await collectors_tag_msg.clear_reactions()
                        await collectors_tag_msg.clear_reactions()
                        await message.clear_reactions()
                        await message.add_reaction(u"\U0001F513")
                        await message.author.add_roles(Pending_role)
                    else:
                        collectors_channel = get(message.guild.text_channels, name='collectors')
                        collectors_role = get(message.guild.roles, name="Collectors")
                        collectors_tag_msg = await collectors_channel.send(collectors_role.mention)
                        embed_collection_log = discord.Embed(
                            title="Gold Collection", description="Run has been posted", color=0x5d4991)
                        embed_collection_log.add_field(
                            name="Author", value=message.author.mention, inline=True)
                        embed_collection_log.add_field(name="Realm: ", value=x[3], inline=True)
                        embed_collection_log.add_field(name="Amount: ", value=x[2], inline=True)
                        embed_collection_log.add_field(
                            name="Channel", value=message.channel.name, inline=False)
                        embed_collection_log.add_field(
                            name="Link", value=message.jump_url, inline=True)
                        embed_collection_log.set_footer(text=datetime.now(timezone.utc).replace(microsecond=0))
                        await collectors_channel.send(embed=embed_collection_log)
                        await collectors_tag_msg.clear_reactions()
                        await message.clear_reactions()
                        await message.add_reaction(u"\U0001F513")
                        await message.author.add_roles(Pending_role)
                else:
                    await message.delete()

        if len(x) == 1:
            if ((not x[0].lower().startswith('dps') and not x[0].lower().startswith('tank') and 
                not x[0].lower().startswith('heal') and not (x[0].lower().startswith('team take') and 
                TeamLeader_role in message.author.roles)) and (message.channel.name.startswith('build-gr') or 
                message.channel.name.startswith('high-keys-gr')) and not roles_check):
                await message.delete()
                    
        if message.author == bot.user and not isinstance(message.channel, discord.DMChannel):
            return
        
        if len(message.mentions) > 0 and not isinstance(message.channel, discord.DMChannel):
            if message.mentions[0] == bot.user:
                await bot.process_commands(message)                
            elif message.content.startswith("!b"):
                await bot.process_commands(message)    
            elif message.content.startswith("!"):
                await bot.process_commands(message)            
        else:
            if message.content.startswith("!") and not isinstance(message.channel, discord.DMChannel):
                if message.content == "!b" and not isinstance(message.channel, discord.DMChannel):
                    await bot.process_commands(message)
                else:
                    await bot.process_commands(message)


@bot.event
async def on_message_delete(message):
    Pending_role = get(message.guild.roles, name='Pending')
    PendingH_role = get(message.guild.roles, name='Pending [H]')
    unlock_emoji = [reaction for reaction in message.reactions if reaction.emoji == u"\U0001F513"]
    if (len(unlock_emoji) == 1 and 
        (
            message.channel.name.startswith('build-group') or message.channel.name.startswith('high-keys-group') or 
            (message.channel.id == 815104637391863857 or message.channel.name == "🔵leveling-torghast-boost") or 
            (message.channel.id == 815104639368298545 or message.channel.name == "🔵rbg-run-submit") or 
            (message.channel.id == 815104639082823699 or message.channel.name == "🔵pvp-build-grp")
        )):
        await message.author.remove_roles(Pending_role)
    if (len(unlock_emoji) == 1 and 
        (
            message.channel.name.startswith('build-grp') or message.channel.name.startswith('high-keys-grp') or
            (message.channel.id == 815104637697916959 or message.channel.name == "🔴leveling-torghast-boost") or 
            (message.channel.id == 815104639661375488 or message.channel.name == "🔴rbg-run-submit") or 
            (message.channel.id == 815104639368298536 or message.channel.name == "🔴pvp-build-grp")
        )):
        await message.author.remove_roles(PendingH_role)


@bot.command()
@commands.has_any_role('Bot Whisperer', 'Management')
async def Stats(ctx, role: discord.Role = None, names=None):
    await ctx.message.delete()
    if role is None:
        stats_channel = get(ctx.guild.text_channels, name='stats')
        everyone_role = get(ctx.guild.roles, name="@everyone")
        members_count = 0
        roles_count = 0
        async for member in ctx.guild.fetch_members():
            members_count += 1
        async for role in ctx.guild.fetch_roles():
            if role == everyone_role:
                continue
            roles_count += 1
            await stats_channel.send(f"Role Name: **\"{role}\"** \nMembers count: **{len(role.members)}**")
            await stats_channel.send("-----------------------------------\n")
        await stats_channel.send("-----------------------------------")
        await stats_channel.send(f"Total Members: {members_count}")
        await stats_channel.send(f"Total Roles: {roles_count}")
    elif names is not None:
        stats_channel = get(ctx.guild.text_channels, name='stats')
        stats_embed = discord.Embed(
            title=f":information_source: {role} Count", 
            description="", 
            color=0x5d4991)
        members_count = 0
        # all_members = ctx.guild.members
        async for member in ctx.guild.fetch_members():
            if role in member.roles:
                if member.nick is not None:
                    await stats_channel.send(member.nick)
                    members_count += 1
                elif member.nick is None:
                    await stats_channel.send(member.name)
                    members_count += 1
        stats_embed.add_field(name="--", value=f"**{members_count}**", inline=True)
        stats_embed.set_footer(text=f"Count as of: {ctx.message.created_at}")
        await stats_channel.send(embed=stats_embed)
    else:
        stats_channel = get(ctx.guild.text_channels, name='stats')
        stats_embed = discord.Embed(
            title=f":information_source: {role} Count", description="", color=0x5d4991)
        members_count = 0
        
        async for member in ctx.guild.fetch_members():
            if role in member.roles:
                members_count += 1
        stats_embed.add_field(name="--", value=f"**{members_count}**", inline=True)
        stats_embed.set_footer(text="Count as of: " + str(ctx.message.created_at))
        await stats_channel.send(embed=stats_embed)


@bot.command()
@commands.has_any_role('Bot Whisperer', 'Management')
async def Realms(ctx, realm=None, names=None):
    await ctx.message.delete()
    if realm is None:
        realms_list = []
        stats_channel = get(ctx.guild.text_channels, name='stats')
        async for member in ctx.guild.fetch_members():
            if member != bot.user and member.nick is not None:
                member_realm = member.nick.partition("-")[2].partition(" [")[0]
                realms_list.append(member_realm)
        realm_counter = collections.Counter(realms_list)
        for x, y in realm_counter.items():
            await stats_channel.send(f"Realm Name: **\"{x}\"** \nMembers count: **{y}**")
            await stats_channel.send("-----------------------------------\n")
    elif names is not None:
        realms_list = []
        stats_channel = get(ctx.guild.text_channels, name='stats')
        realm_embed = discord.Embed(
            title=f":information_source: {realm} Count", description="", color=0x5d4991)
        
        async for member in ctx.guild.fetch_members():
            if member != bot.user and member.nick is not None:
                member_realm = member.nick.partition("-")[2].partition(" [")[0]
                if member_realm == realm:
                    realms_list.append(member_realm)
                    await stats_channel.send(member.nick)
        if realm in realms_list:
            realm_counter = collections.Counter(realms_list)
            for x, y in realm_counter.items():
                realm_embed.add_field(name="--", value=f"**{y}**", inline=True)
                realm_embed.set_footer(text="Count as of: " + str(ctx.message.created_at))
                await stats_channel.send(embed=realm_embed)
    else:
        realms_list = []
        stats_channel = get(ctx.guild.text_channels, name='stats')
        realm_embed = discord.Embed(
            title=f":information_source: {realm} Count", description="", color=0x5d4991)
        async for member in ctx.guild.fetch_members():
            if member != bot.user and member.nick is not None:
                member_realm = member.nick.partition("-")[2].partition(" [")[0]
                if member_realm == realm:
                    realms_list.append(member_realm)
        if realm in realms_list:
            realm_counter = collections.Counter(realms_list)
            for x, y in realm_counter.items():
                realm_embed.add_field(name="--", value=f"**{y}**", inline=True)
                realm_embed.set_footer(text="Count as of: " + str(ctx.message.created_at))
                await stats_channel.send(embed=realm_embed)
        else:
            realm_embed.add_field(
                name="--", value=f"**There is no members from {realm}**", inline=True)
            realm_embed.set_footer(text="Count as of: " + str(ctx.message.created_at))
            await stats_channel.send(embed=realm_embed)


@bot.command()
@commands.has_any_role('Moderator', 'NOVA')
async def CheckLog(ctx, tgt_user):
    """To check audit log entries for given member
    example : !CheckLog "ASLK76#2188"
    """
    await ctx.message.delete()
    async for entry in ctx.guild.audit_logs(limit=None):
        # await ctx.send(entry.target)
        # if tgt_user == entry.target:
        #    await ctx.send('{0.user} did {0.action} to {0.target}'.format(entry))
        if tgt_user == str(entry.target):
            await ctx.send(
                f"Date: {entry.created_at} : {entry.user.display_name} did "
                f"{entry.action} to {entry.target}, the following changed: {entry.changes}")


@bot.command()
@commands.has_any_role('Bot Whisperer', 'Management')
async def Decline(ctx, user: discord.Member):
    """To send decline message in DM to any applicant
    example : !Decline @ASLK76#2188
    """
    await user.send(
        f"Hello **{user.name}**\n\nThank you for submitting an application to become a booster for ***NOVA***, "
        "however on this occasion we regret to inform you that your application has been declined.\n"
        "\nThank you,"
        "***NOVA Team***")


@bot.command()
@commands.has_any_role('Moderator', 'Management')
async def echo(ctx, channel: discord.TextChannel, *, msg):
    """To send a message in a channel from the bot
    example : !echo #staff Hello, this is your bot speaking
    """
    await ctx.message.delete()
    await channel.send(msg)


@bot.command(aliases=['b_xfaction', 'bal_cross', 'b_cross'])
async def balance_command_crossfaction(ctx, *, target_booster=None):
    """To check balance of booster cross faction name
    """
    await ctx.message.delete()
    if target_booster is None:
        name, realm = await checkPers(ctx.author.id)
        if name is None:
            name, realm = ctx.author.nick.split("-")

        balance_name = f"{name}-{realm}"
    else:
        Moderator_role = get(ctx.guild.roles, name="Moderator")
        Management_role = get(ctx.guild.roles, name="Management")
        if Moderator_role in ctx.author.roles or Management_role in ctx.author.roles:
            balance_name = target_booster
            ctx.command.reset_cooldown(ctx)
        else:
            return await ctx.send("You don't have permissions to check other members balance")

    balance_check_channel = get(ctx.guild.text_channels, id=815104636251275312)
    if ctx.message.channel.id != 815104636251275312:
        return await ctx.message.channel.send(
            f"Head to {balance_check_channel.mention} to issue the command", 
            delete_after=5)
    if not (balance_name.endswith("[A]") or balance_name.endswith("[H]")):
        return await ctx.send("Invalid name format")
    try:
        if balance_name.endswith("[A]"):
            balance_name = search_nested_horde(boosters, ctx.message.author.nick)       
        elif balance_name.endswith("[H]"):
            balance_name = search_nested_alliance(boosters, ctx.message.author.nick)

        async with ctx.bot.mplus_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = """
                    SELECT cur_balance, pre_balance, tot_balance 
                    FROM ov_creds 
                    WHERE booster=%s
                """
                val = (balance_name,)
                await cursor.execute(query, val)
                balance_result = await cursor.fetchall()
                if len(balance_result) > 0 :
                    cur_bal, pre_bal, tot_bal = balance_result
                else:
                    cur_bal = pre_bal = tot_bal = 0

                current_balance = f"🏧  {cur_bal:,}"
                previous_balance = f"🏧  {pre_bal:,}"
                total_balance = f"🏧  {tot_bal:,}"

                await ctx.send(f"{ctx.message.author.mention} balance has been sent in a DM", 
                                delete_after=3)
                balance_embed = discord.Embed(title="Balance Info!",
                                                description=f"{balance_name}",
                                                color=0xffd700)
                balance_embed.add_field(name="Current Balance",
                                        value=current_balance, inline=False)
                balance_embed.add_field(name="Previous Balance",
                                        value=previous_balance, inline=False)
                balance_embed.add_field(name="Total Balance",
                                        value=total_balance, inline=False)
                await ctx.author.send(embed=balance_embed)

    except discord.errors.Forbidden:
        await ctx.send(
            f"{ctx.message.author.mention} cannot send you a DM, please allow DM's from server members", 
            delete_after=5)


def check_if_it_stan_me(ctx):
    return (ctx.message.author.id == 226069789754392576 or 
            ctx.message.author.id == 163324686086832129 or 
            ctx.message.author.id == 186433880872583169)


@bot.command()
@commands.check(check_if_it_stan_me)
async def setTroll(ctx, target: int):
    await ctx.message.delete()
    global troll_target
    troll_target = target


@bot.command()
@commands.check(check_if_it_stan_me)
async def setTroll1(ctx, target: int):
    await ctx.message.delete()
    global troll_target1
    troll_target1 = target


@bot.command()
@commands.check(check_if_it_stan_me)
async def setTroll2(ctx, target: int):
    await ctx.message.delete()
    global troll_target2
    troll_target2 = target


@bot.command()
@commands.check(check_if_it_stan_me)
async def setTroll3(ctx, target: int):
    await ctx.message.delete()
    global troll_target3
    troll_target3 = target


@bot.command()
@commands.has_any_role('Bot Whisperer', 'Management')
async def CheckPvp(ctx, user: discord.Member, name, realm):
    await ctx.message.delete()
    pvps_channel = get(ctx.guild.text_channels, name='pvps-channel')
    response = requests.get(
        "https://raider.io/api/v1/characters/profile?region=eu&"
        f"realm={realm}&name={name}&fields=mythic_plus_scores_by_season%3Acurrent%3Aseason-bfa-3")
    if response.status_code == 200:
        json_str = json.dumps(response.json())
        resp = json.loads(json_str)
        char_class = resp['class']
        char_faction = resp['faction']
        realm_pre = realm.capitalize().replace(' ', '')
        if realm_pre.startswith("Pozzo"):
            realm_final = "Pozzo"
        elif realm_pre == "Dunmodr":
            realm_final = "DunModr"
        elif realm_pre == "Twistingnether":
            realm_final = "TwistingNether"
        elif realm_pre == "Tarrenmill":
            realm_final = "TarrenMill"
        elif realm_pre == "Colinaspardas":
            realm_final = "ColinasPardas"
        elif realm_pre == "Burninglegion":
            realm_final = "BurningLegion"
        elif realm_pre == "Themaelstrom":
            realm_final = "TheMaelstrom"
        elif realm_pre == "Defiasbrotherhood":
            realm_final = "Defias"
        elif realm_pre == "Shatteredhand":
            realm_final = "Shattered"
        elif realm_pre == "Argentdawn":
            realm_final = "ArgentDawn"
        elif realm_pre == "Burningblade":
            realm_final = "BurningBlade"
        else:
            realm_final = realm_pre
        await user.edit(nick=f"{name.capitalize()}-{realm_final} [{char_faction[0].upper()}]")
        roles_toadd = [get(ctx.guild.roles, name=char_faction.capitalize()), 
            get(ctx.guild.roles, name=char_class.capitalize()), 
            get(ctx.guild.roles, name="PVP"), 
            get(ctx.guild.roles, name="EU")]
        await user.add_roles(*roles_toadd)
        if realm == "sanguino" or realm == "uldum" or realm == "Shen'dralar" or realm == "zul'jin":
            await user.add_roles(get(ctx.guild.roles, name="SPA"))
        if realm == "pozzo-delleternità":
            await user.add_roles(get(ctx.guild.roles, name="ITA"))
        if realm == "hyjal":
            await user.add_roles(get(ctx.guild.roles, name="Hyjal"))
        if realm == "dalaran" or realm.startswith("Marécage"):
            await user.add_roles(get(ctx.guild.roles, name="Dalaran"))
        if realm == "Drak'thul":
            await user.add_roles(get(ctx.guild.roles, name="Drak'thul"))
        if realm == "Exodar":
            await user.add_roles(get(ctx.guild.roles, name="Exodar"))
        if realm == "TheMaelstrom":
            await user.add_roles(get(ctx.guild.roles, name="The-Maelstrom"))
        await user.create_dm()
        await user.dm_channel.send(
            f"Hello **{user.name}** :slight_smile:\nYour application for ***NOVA*** as a booster has been "
            "accepted, **CHECK YOUR ROLES** and__ please read all our channels before taking further steps "
            "inside the community.__\n \n**# ow-payment-works** Important info regarding how payment works;\n"
            "**# Rules** That you must follow at all costs or your risk yourself getting Strike / banned; \n"
            "**# old-balance** To check your weekly earnings;\n"
            "**# Payday-info** To see when the next payment wave will be done;\n"
            "**# ick-your-roles** To request further roles (you don't need to go through the google form "
            "once inside);\n \nThank you,\n"
            "***NOVA Team***")
        if get(ctx.guild.roles, name="Client") in user.roles:
            await user.remove_roles(get(ctx.guild.roles, name="Client"))
        if get(ctx.guild.roles, name="PickYourRegion") in user.roles:
            await user.remove_roles(get(ctx.guild.roles, name="PickYourRegion"))
        await pvps_channel.send(f"`{name.capitalize()}-{realm_final} "
                                f"[{char_faction[0].upper()}] Accepted and given the ranks`")
    else:
        await pvps_channel.send(f"No records found for {name.capitalize()}-{realm.capitalize().replace(' ', '')} "
                                f"on raider.io")


@bot.command()
@commands.has_any_role('RBG Leader', 'Management', 'Moderator')
async def CheckRbg(ctx, user: discord.Member, name, realm):
    await ctx.message.delete()
    rbg_channel = get(ctx.guild.text_channels, id=804513656271667220)
    
    response = requests.get(
        "https://raider.io/api/v1/characters/profile?region=eu&"
        f"realm={realm}&name={name}&fields=mythic_plus_scores_by_season%3Acurrent")
    if response.status_code == 200:
        json_str = json.dumps(response.json())
        resp = json.loads(json_str)
        # char_class = resp['class']
        char_faction = resp['faction']
        realm_pre = realm.capitalize().replace(' ', '')
        if realm_pre.startswith("Pozzo"):
            realm_final = "Pozzo"
        elif realm_pre == "Dunmodr":
            realm_final = "DunModr"
        elif realm_pre == "Twistingnether":
            realm_final = "TwistingNether"
        elif realm_pre == "Tarrenmill":
            realm_final = "TarrenMill"
        elif realm_pre == "Colinaspardas":
            realm_final = "ColinasPardas"
        elif realm_pre == "Burninglegion":
            realm_final = "BurningLegion"
        elif realm_pre == "Themaelstrom":
            realm_final = "TheMaelstrom"
        elif realm_pre == "Defiasbrotherhood":
            realm_final = "Defias"
        elif realm_pre == "Shatteredhand":
            realm_final = "Shattered"
        elif realm_pre == "Argentdawn":
            realm_final = "ArgentDawn"
        elif realm_pre == "Burningblade":
            realm_final = "BurningBlade"
        elif realm_pre.startswith("Aggra-"):
            realm_final = "Aggra"
        else:
            realm_final = realm_pre
        await user.edit(nick=f"{name.capitalize()}-{realm_final} [{char_faction[0].upper()}]")
        roles_toadd = [get(ctx.guild.roles, name=char_faction.capitalize()),
                        get(ctx.guild.roles, name="RBG"),
                        get(ctx.guild.roles, name="EU")]
        await user.add_roles(*roles_toadd)
        if realm_final.lower().startswith("pozzo") and char_faction.lower() == "alliance":
            await user.add_roles(get(ctx.guild.roles, name="ITA"))
        elif realm_final.lower().startswith("pozzo") and char_faction.lower() == "horde":
            await user.add_roles(get(ctx.guild.roles, name="ITA [H]"))
        elif realm_final.lower() == "hyjal":
            await user.add_roles(get(ctx.guild.roles, name="Hyjal"))
        elif realm_final.lower() == "dalaran" or realm_final.lower().startswith("marécage"):
            await user.add_roles(get(ctx.guild.roles, name="Dalaran"))
        elif realm_final.lower() == "drak'thul" and char_faction.lower() == "alliance":
            await user.add_roles(get(ctx.guild.roles, name="Drak'thul"))
        elif realm_final.lower() == "exodar":
            await user.add_roles(get(ctx.guild.roles, name="Exodar"))
        elif realm_final.lower() == "themaelstrom":
            await user.add_roles(get(ctx.guild.roles, name="The-Maelstrom"))
        elif ((realm_final.lower() == "drak'thul" or realm_final.lower() == "burningblade") and 
            char_faction.lower() == "horde"):
            await user.add_roles(get(ctx.guild.roles, name="DB"))
        elif ((realm_final.lower() == "frostmane" or realm_final.lower() == "grimbatol" or 
            realm_final.lower() == "aggra") and char_faction.lower() == "horde"):
            await user.add_roles(get(ctx.guild.roles, name="GFA"))

        await user.create_dm()
        await user.dm_channel.send(
            f"Hello **{user.name}** :slight_smile:\nYour application for ***NOVA*** as a booster has been "
            "accepted, **CHECK YOUR ROLES** and__ please read all our channels before taking further steps "
            "inside the community.__\n \n**# Information** Important info regarding how payment works;\n"
            "**# Rules** That you must follow at all costs or your risk yourself getting Strike / banned; \n"
            "**# Balance-check** To check your weekly earnings;\n"
            "**# Payday-info** To see when the next payment wave will be done;\n"
            "**# Pick-your-roles** To request further roles (you don't need to go through the google form "
            "once inside);\n \nThank you,\n"
            "***NOVA Team***")
        if get(ctx.guild.roles, name="Client") in user.roles:
            await user.remove_roles(get(ctx.guild.roles, name="Client"))
        if get(ctx.guild.roles, name="PickYourRegion") in user.roles:
            await user.remove_roles(get(ctx.guild.roles, name="PickYourRegion"))
        await rbg_channel.send(f"`{name.capitalize()}-{realm_final} "
                                f"[{char_faction[0].upper()}] Accepted and given the ranks`")
    else:
        await rbg_channel.send(f"No records found for {name.capitalize()}-{realm.capitalize().replace(' ', '')} "
                                f"on raider.io")
    

@bot.command()
@commands.has_any_role('Moderator', 'Staff', 'Management')
async def ImportRaids(ctx, *, pastebin_url):
    """To manually import raids from the sheet to DB
    example : nm!ImportRaids https://pastebin.com/raw/JfHxJrAG
    """
    await ctx.message.delete()
    raid_vals = []    
    response = requests.get(pastebin_url)
    response.encoding = "utf-8"
    body = response.content.decode("utf-8")
    raid_names = body.replace("\r","").split("\n")
    for i in raid_names:
        name, realm, amount = i.split("\t")
        raid_vals.append([name, realm, amount.replace(",","")])
    async with ctx.bot.mplus_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("DELETE FROM `raid_balance`")
            await cursor.execute("ALTER TABLE `raid_balance` AUTO_INCREMENT = 1")
            query = """
                INSERT INTO `raid_balance` (`name`, `realm`,`amount`) 
                    VALUES (%s, %s, %s)
            """
            await cursor.executemany(query, raid_vals)
            await ctx.send(
                f"{cursor.rowcount} Records inserted successfully into raid_balance table",
                delete_after=10)


# region code from MPlus bot
@bot.command()
@commands.has_any_role('Moderator', 'Staff', 'Management')
async def EditPot(ctx, boostid :int , boost_type, amount):
    """To edit run pots
       The command structure is `!EditPot <boostid> <boost_type> <amount>` - <boost_type> is `mplus` for M+ or `various` for the rest.
    """
    await ctx.message.delete()
    track_channel = get(ctx.guild.text_channels, id=840733014622601226)
    amount = convert_si_to_number(amount.replace(",", "."))
    async with ctx.bot.mplus_pool.acquire() as conn:
        if boost_type == "mplus":
            async with conn.cursor() as cursor:
                query  = """
                    SELECT * FROM m_plus where boost_id = %s 
                        AND deleted_at IS NULL or deleted_at = ' '
                """
                val = (boostid,)
                await cursor.execute(query, val)
                notSoftDeleted = await cursor.fetchone()
                
            if notSoftDeleted is not None:
                async with conn.cursor() as cursor:
                    query  = """
                        SET @adv_cut := (SELECT adv_cut/boost_pot FROM m_plus WHERE boost_id = %(boost_id)s LIMIT 1);
                        UPDATE m_plus SET
                            boost_pot  = %(amount)s,
                            edited_at  = UTC_TIMESTAMP(),
                            adv_cut    = @adv_cut * %(amount)s,
                            tank_cut   = %(amount)s * 0.175,
                            healer_cut = %(amount)s * 0.175,
                            dps1_cut   = %(amount)s * 0.175,
                            dps2_cut   = %(amount)s * 0.175

                        WHERE boost_id = %(boost_id)s
                    """
                    val = {"amount": amount, "boost_id": boostid}
                    await cursor.execute(query, val)
                    em = discord.Embed(title="MPlus Pot Changed",
                                                description=
                                                    f"The pot for the m_plus run with ID **{boostid}** was edited to **{amount}** "
                                                    f"by {ctx.message.author.mention}",
                                                color=discord.Color.orange())
                    await track_channel.send(embed=em)
                    await ctx.author.send(embed=em)
            else:
                await ctx.author.send(f"The {boost_type} run with ID {boostid} wasn't found in the Database or it was already deleted.")

        elif boost_type == "various":
            async with conn.cursor() as cursor:
                query  = "SELECT * FROM various where boost_id = %s AND deleted_at IS NULL or deleted_at = ' '"
                val = (boostid,)
                await cursor.execute(query, val)
                notSoftDeleted = await cursor.fetchone()

            if notSoftDeleted is not None:
                async with conn.cursor() as cursor:
                    query  = """
                        SET @adv_cut := (SELECT adv_cut/boost_pot FROM various WHERE boost_id = %(boost_id)s LIMIT 1);
                        UPDATE various SET
                            boost_pot  = %(amount)s,
                            edited_at  = UTC_TIMESTAMP(),
                            adv_cut    = @adv_cut * %(amount)s,
                            tank_cut   = %(amount)s * 0.175

                        WHERE boost_id = %(boost_id)s
                    """
                    val = {"amount": amount, "boost_id": boostid}
                    await cursor.execute(query, val)
                    em = discord.Embed(title="Various Pot Changed",
                                                description=
                                                    f"The pot for the various run with ID **{boostid}** was edited to **{amount}** "
                                                    f"by {ctx.message.author.mention}",
                                                color=discord.Color.orange())
                    await track_channel.send(embed=em)
                    await ctx.author.send(embed=em)
            else:
                await ctx.author.send(f"The {boost_type} run with ID {boostid} wasn't found in the Database or it was already deleted.")


@bot.command()
@commands.has_any_role('Moderator', 'Collectors', 'Staff')
async def Collected(ctx, pot, adv, realm, *, desc):
    """To enter manual collections
       example: !Collected 100K "Advertiser-Kazzak [H]" "Draenor [H]" description of the collection
    """
    await ctx.message.delete()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    async with ctx.bot.mplus_pool.acquire() as conn:
        if ctx.channel.name == "collectors":
            collected_embed = discord.Embed(
                title=":information_source: Gold Collection", 
                description="", color=0x87cefa)
            collected_embed.add_field(name="**Collected By: **",
                value=ctx.author.mention, inline=True)
            collected_embed.add_field(name="**Pot: **", 
                value=pot, inline=True)
            collected_embed.add_field(name="**From: **", 
                value=adv, inline=True)
            collected_embed.add_field(name="**On: **", 
                value=realm, inline=True)
            collected_embed.add_field(name="**Run description: **", 
                value=desc, inline=False)
            collected_msg = await ctx.send(embed=collected_embed)
            await collected_msg.clear_reactions()
            await collected_msg.clear_reactions()
            await collected_msg.add_reaction(u"\U0001F4B8")

            def check(reaction, user):
                m = collected_msg
                return user == ctx.message.author and str(reaction.emoji) == u"\U0001F4B8" and m.id == reaction.message.id

            try:
                reaction, user = await bot.wait_for('reaction_add', check=check)
            except Exception:
                pass
            else:
                name, realm = await checkPers(user.id)
                if name is None:
                    if "-" not in user.nick:
                        raise ValueError(f"Nickname format not correct for {user.display_name}")
                    name, realm = user.nick.split("-")

                user_final = f"{name}-{realm}"
                async with conn.cursor() as cursor:
                    query = """
                        INSERT INTO collectors 
                            (collection_id, collector, trialadv, realm, amount, date_collected) 
                            VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    val = (collected_msg.id, user_final, adv, realm, pot, now)
                    await cursor.execute(query, val)
                    await collected_msg.add_reaction(u"\U0001F5C4")
    

@bot.command()
@commands.has_any_role('Moderator', 'Staff', 'Management')
async def RemoveRun(ctx, boostid :int, boost_type):
    """To remove boost run
       The command structure is `!RemoveRun <boostid> <boost_type>` 
       - <boost_type> is `mplus` for M+ or `various` for the rest.
    """
    await ctx.message.delete()
    track_channel = get(ctx.guild.text_channels, id=840733014622601226)
    async with ctx.bot.mplus_pool.acquire() as conn:
        if boost_type == "mplus":
            async with conn.cursor() as cursor:
                query = """
                    SELECT * FROM m_plus where boost_id = %s
                        AND deleted_at IS NULL or deleted_at = ' '
                """
                val = (boostid,)
                await cursor.execute(query, val)
                notSoftDeleted = await cursor.fetchone()

                if notSoftDeleted is not None:
                    async with conn.cursor() as cursor:
                        query  = """
                            UPDATE m_plus SET 
                                DELETED_AT = UTC_TIMESTAMP() 
                                WHERE boost_id = %s
                        """
                        val = (boostid,)
                        await cursor.execute(query, val)
                        em = discord.Embed(title="MPlus Run Removed",
                                                description=
                                                    f"The run with ID **{boostid}** was removed "
                                                    f"by {ctx.message.author.mention}",
                                                color=discord.Color.orange())
                        await track_channel.send(embed=em)
                        await ctx.author.send(embed=em)
                else:
                    await ctx.author.send(f"The run with ID {boostid} wasn't found in the Database or it was already deleted.")
        elif boost_type == "various":
            async with conn.cursor() as cursor:
                query = """
                    SELECT * FROM various where boost_id = %s
                        AND deleted_at IS NULL or deleted_at = ' '
                """
                val = (boostid,)
                await cursor.execute(query, val)
                notSoftDeleted = await cursor.fetchone()

                if notSoftDeleted is not None:
                    async with conn.cursor() as cursor:
                        query  = """
                            UPDATE various SET DELETED_AT = UTC_TIMESTAMP() where boost_id = %s
                        """
                        val = (boostid,)
                        await cursor.execute(query, val)
                        em = discord.Embed(title="MPlus Run Removed",
                                                description=
                                                    f"The run with ID **{boostid}** was removed "
                                                    f"by {ctx.message.author.mention}",
                                                color=discord.Color.orange())
                        await track_channel.send(embed=em)
                        await ctx.author.send(embed=em)
                else:
                    await ctx.author.send(f"The run with ID {boostid} wasn't found in the Database or it was already deleted.")


@bot.command()
@commands.has_any_role('Moderator', 'Staff', 'Management')
async def RemoveCollection(ctx, collectionid :int):
    """To remove Collection
       The command structure is `!RemoveCollection <collectionid>`
    """
    await ctx.message.delete()
    track_channel = get(ctx.guild.text_channels, id=840733014622601226)
    async with ctx.bot.mplus_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            query = """
                SELECT * FROM collectors where collection_id = %s
                    AND deleted_at IS NULL or deleted_at = ' '
            """
            val = (collectionid,)
            await cursor.execute(query, val)
            notSoftDeleted = await cursor.fetchone()

            if notSoftDeleted is not None:
                async with conn.cursor() as cursor:
                    query  = """
                        UPDATE collectors SET DELETED_AT = UTC_TIMESTAMP() where collection_id = %s
                    """
                    val = (collectionid,)
                    await cursor.execute(query, val)
                    em = discord.Embed(title="MPlus Run Removed",
                                                description=
                                                    f"The collection with ID **{collectionid}** was removed "
                                                    f"by {ctx.message.author.mention}",
                                                color=discord.Color.orange())
                    await track_channel.send(embed=em)
                    await ctx.author.send(embed=em)
            else:
                await ctx.author.send(f"The collection with ID {collectionid} wasn't found in the Database or it was already deleted.")


@bot.command()
@commands.has_any_role('Moderator', 'Staff', 'Management')
async def EditRunCut(ctx, boostid :int, boost_type, booster, amount):
    """To edit M+ adv/boosters cut
       
       <boost_type> is `mplus` for M+ or `various` for the rest.

       -for `mplus` <booster_role> are: adv, tank, healer, dps1, dps2
       -for `various` <booster_role> are: adv, tank

       example: !EditRunCut 839463136980631552 mplus tank 100K
    """
    await ctx.message.delete()
    amount = convert_si_to_number(amount.replace(",", "."))
    track_channel = get(ctx.guild.text_channels, id=840733014622601226)
    async with ctx.bot.mplus_pool.acquire() as conn:
        if boost_type == "mplus":
            if booster not in ['adv','tank','healer','dps1','dps2']:
                await ctx.author.send("Check your input regarding the role."
                                    f" I only accept adv, tank, healer, dps1 or dps2. You typed {booster}")
            else:
                async with conn.cursor() as cursor:
                    query  = """
                    SELECT * FROM m_plus where boost_id = %s 
                        AND deleted_at IS NULL or deleted_at = ' '
                    """
                    val = (boostid,)
                    await cursor.execute(query, val)
                    notSoftDeleted = await cursor.fetchall()
                    if notSoftDeleted is not None:
                        async with conn.cursor() as cursor:
                            query  = (f"UPDATE m_plus SET "
                                        f"edited_at = UTC_TIMESTAMP(), "
                                        f"{booster}_cut   = %(amount)s "
                                        f"WHERE boost_id = %(boost_id)s")
                            val = {"amount": amount, "boost_id": boostid}
                            await cursor.execute(query, val)

                            query  = (f"SELECT CONCAT({booster}_name, '-', {booster}_realm) "
                                    f"FROM m_plus where boost_id = %s")
                            val = (boostid,)
                            await cursor.execute(query, val)
                            (booster_name,) = await cursor.fetchone()
                            em = discord.Embed(title="MPlus Booster Pot Changed",
                                                description=
                                                    f"The pot for **{booster_name}** was edited to **{amount}** "
                                                    f"for the boost run with ID **{boostid}** by {ctx.message.author.mention}",
                                                color=discord.Color.orange())
                            await track_channel.send(embed=em)
                            await ctx.author.send(embed=em)
                    else:
                        await ctx.author.send(f"The boost run with ID {boostid} doesn't exist in the Database.")

        elif boost_type == "various":
            if booster not in ['adv','tank']:
                await ctx.author.send("Check your input regarding the role."
                                    f" I only accept adv, tank. You typed {booster}")
            else:
                async with conn.cursor() as cursor:
                    query  = """
                        SELECT * FROM various where boost_id = %s 
                            AND deleted_at IS NULL or deleted_at = ' '
                    """
                    val = (boostid,)
                    await cursor.execute(query, val)
                    notSoftDeleted = await cursor.fetchall()
                    if notSoftDeleted is not None:
                        async with conn.cursor() as cursor:
                            query  = (f"UPDATE various SET "
                                    f"edited_at  = UTC_TIMESTAMP(),"
                                    f"{booster}_cut   = %(amount)s "
                                    f"WHERE boost_id = %(boost_id)s")
                            val = {"amount": amount, "boost_id": boostid}
                            await cursor.execute(query, val)

                            query  = (f"SELECT CONCAT({booster}_name, '-', {booster}_realm) "
                                    f"FROM various where boost_id = %s")
                            val = (boostid,)
                            await cursor.execute(query, val)
                            (booster_name,) = await cursor.fetchone()
                            em = discord.Embed(title="Various Booster Pot Changed",
                                                description=
                                                    f"The pot for **{booster_name}** was edited to **{amount}** "
                                                    f"for the boost run with ID **{boostid}** by {ctx.message.author.mention}",
                                                color=discord.Color.orange())
                            await track_channel.send(embed=em)
                            await ctx.author.send(embed=em)
                    else:
                        await ctx.author.send(f"The boost run with ID {boostid} doesn't exist in the Database.")


@bot.command()
@commands.has_any_role('Moderator', 'Staff', 'Management')
async def EditRunBooster(ctx, boostid :int, boost_type, booster_role, name, *, realm):
    """To edit M+ adv/boosters name
       
       -<boost_type> is `mplus` for M+ or `various` for the rest.

       -for `mplus` <booster_role> are: adv, tank, healer, dps1, dps2
       -for `various` <booster_role> are: adv, tank

       example: !EditRunBooster 839463136980631552 mplus tank Sanfura TarrenMill [H]
       
       Please make sure you copy paste the correct booster name and realm
    """
    await ctx.message.delete()
    track_channel = get(ctx.guild.text_channels, id=840733014622601226)
    async with ctx.bot.mplus_pool.acquire() as conn:
        if boost_type == "mplus":
            if booster_role not in ['adv','tank','healer','dps1','dps2']:
                await ctx.author.send("Check your input regarding the role."
                                    f" I only accept adv, tank, healer, dps1 or dps2. You typed {booster_role}")
            else:
                async with conn.cursor() as cursor:
                    query  = """
                    SELECT * FROM m_plus where boost_id = %s 
                        AND deleted_at IS NULL or deleted_at = ' '
                    """
                    val = (boostid,)
                    await cursor.execute(query, val)
                    notSoftDeleted = await cursor.fetchall()
                    if notSoftDeleted is not None:                          
                        async with conn.cursor() as cursor:
                            query  = (f"SELECT CONCAT({booster_role}_name, '-', {booster_role}_realm) "
                                    f"FROM m_plus where boost_id = %s")
                            val = (boostid,)
                            await cursor.execute(query, val)
                            (previous_booster,) = await cursor.fetchone()

                            if previous_booster == f"{name}-{realm}":
                                await ctx.author.send(f"The booster with the name {name}-{realm} is already changed")
                            else:
                                query  = (f"UPDATE m_plus SET "
                                            f"edited_at            = UTC_TIMESTAMP(), "
                                            f"{booster_role}_name  = %(name)s, "
                                            f"{booster_role}_realm = %(realm)s "
                                            f"WHERE boost_id       = %(boost_id)s")
                                val = {"name": name, "realm": realm, "boost_id": boostid}
                                await cursor.execute(query, val)
                                em = discord.Embed(title="MPlus Booster Changed",
                                                    description=
                                                        f"The {booster_role.capitalize()} for run with ID {boostid} "
                                                        f"was edited from **{previous_booster}** to **{name}-{realm}** "
                                                        f"by {ctx.message.author.mention}",
                                                    color=discord.Color.orange())
                                await track_channel.send(embed=em)
                                await ctx.author.send(embed=em)
                    else:
                        await ctx.author.send(f"The boost run with ID {boostid} doesn't exist in the Database.")

        elif boost_type == "various":
            if booster_role not in ['adv','tank']:
                await ctx.author.send("Check your input regarding the role."
                                    f" I only accept adv, tank. You typed {booster_role}")
            else:
                async with conn.cursor() as cursor:
                    query  = """
                        SELECT * FROM various where boost_id = %s 
                            AND deleted_at IS NULL or deleted_at = ' '
                    """
                    val = (boostid,)
                    await cursor.execute(query, val)
                    notSoftDeleted = await cursor.fetchall()
                    if notSoftDeleted is not None:
                        async with conn.cursor() as cursor:
                            query  = (f"SELECT CONCAT({booster_role}_name, '-', {booster_role}_realm) "
                                    f"FROM various where boost_id = %s")
                            val = (boostid,)
                            await cursor.execute(query, val)
                            (previous_booster,) = await cursor.fetchone()
                            if previous_booster == f"{name}-{realm}":
                                await ctx.author.send(f"The booster with the name {name}-{realm} is already changed")
                            else:
                                query  = (f"UPDATE various SET "
                                            f"edited_at            = UTC_TIMESTAMP(), "
                                            f"{booster_role}_name  = %(name)s, "
                                            f"{booster_role}_realm = %(realm)s "
                                            f"WHERE boost_id       = %(boost_id)s")
                                val = {"name": name, "realm": realm, "boost_id": boostid}
                                await cursor.execute(query, val)
                                em = discord.Embed(title="Various Booster Changed",
                                                    description=
                                                        f"The {booster_role.capitalize()} for run with ID {boostid} "
                                                        f"was edited from **{previous_booster}** to **{name}-{realm}** "
                                                        f"by {ctx.message.author.mention}",
                                                    color=discord.Color.orange())
                                await track_channel.send(embed=em)
                                await ctx.author.send(embed=em)
                    else:
                        await ctx.author.send(f"The boost run with ID {boostid} doesn't exist in the Database.")


@bot.command()
@commands.has_any_role('Bot Whisperer', 'Management')
async def Strike(ctx, user: discord.Member, amount, *, reason):
    """To strike a booster run
       example : !Strike @ASLK76#2188 100K depleting a key 
    """
    await ctx.message.delete()
    async with ctx.bot.mplus_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            strike_channel = get(ctx.guild.text_channels, name='strike-channel')
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            name, realm = await checkPers(user.id)
            if name is None:
                if "-" not in user.nick:
                    raise ValueError(f"Nickname format not correct for {user.display_name}")
                name, realm = user.nick.split("-")

            if not amount.startswith('-'):
                command_amount = convert_si_to_number(amount.replace(",", ".")) * -1
            else:
                command_amount = convert_si_to_number(amount.replace(",", "."))

            query = """
                INSERT INTO balance_ops
                    (operation_id, date, name, realm, operation, command, reason, amount, author)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            val = (ctx.message.id, now, name, realm, 'Deduction', 'Strike', reason, command_amount, ctx.message.author.display_name)
            await cursor.execute(query, val)
            if command_amount != 0:
                await strike_channel.send(
                    f"{user.mention}, {reason}. {abs(command_amount):,d}"
                    f" will be deducted from your balance. Strike ID: {ctx.message.id}")
            else:
                await strike_channel.send(
                    f"{user.mention}, {reason}. Strike ID: {ctx.message.id}")


@bot.command()
@commands.has_any_role('Staff', 'Management')
async def AddBalance(ctx, user: discord.Member, amount, *, reason):
    """To add balance to anyone.
       example: !AddBalance @ASLK76#2188 100K being awesome
    """
    await ctx.message.delete()
    async with ctx.bot.mplus_pool.acquire() as conn:
        track_channel = get(ctx.guild.text_channels, id=840733014622601226)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        name, realm = await checkPers(user.id)
        if name is None:
            if "-" not in user.nick:
                raise ValueError(f"Nickname format not correct for {user.display_name}")
            name, realm = user.nick.split("-")

        async with conn.cursor() as cursor:
            command_add = convert_si_to_number(amount.replace(",", "."))
            query = """
                INSERT INTO balance_ops 
                    (operation_id, date, name, realm, operation, command, reason, amount, author) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            val = (ctx.message.id, now, name, realm, 'Add', 'AddBalance', reason, command_add, ctx.message.author.display_name)
            await cursor.execute(query, val)
            em = discord.Embed(title="Balance Added",
                                description=
                                    f"{user.mention} got added **{command_add:,d}** gold because "
                                    f"**{reason}** by {ctx.message.author.mention}."
                                    f"Add ID: {ctx.message.id}",
                                color=discord.Color.orange())
            await track_channel.send(embed=em)
            await ctx.author.send(embed=em)


@bot.command()
@commands.has_any_role('Staff', 'Management')
async def DeductBalance(ctx, user: discord.Member, amount: str, *, reason: str):
    """To deduct balance from anyone.
       example: !DeductBalance @ASLK76#2188 100K in house boost payment
    """
    await ctx.message.delete()
    balance_channel = get(ctx.guild.text_channels, id=840733014622601226)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    async with ctx.bot.mplus_pool.acquire() as conn:
        name, realm = await checkPers(user.id)
        if name is None:
            if "-" not in user.nick:
                raise ValueError(f"Nickname format not correct for {user.display_name}")
            name, realm = user.nick.split("-")

        async with conn.cursor() as cursor:
            if not amount.startswith('-'):
                command_deduct = convert_si_to_number(amount.replace(",", ".")) * -1
            else:
                command_deduct = convert_si_to_number(amount.replace(",", "."))

            query = """
                INSERT INTO balance_ops 
                    (operation_id, date, name, realm, operation, command, reason, amount, author) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            val = (ctx.message.id, now, name, realm, 'Deduction', 'RemoveBalance', reason, command_deduct, ctx.message.author.display_name)
            await cursor.execute(query, val)
            em = discord.Embed(title="Balance Deducted",
                                description=
                                    f"{user.mention} got deducted **{command_deduct:,d}** gold because "
                                    f"**{reason}** by {ctx.message.author.mention}."
                                    f"Deduct ID: {ctx.message.id}",
                                color=discord.Color.orange())
            await balance_channel.send(embed=em)
            await ctx.author.send(embed=em)


@bot.command()
@commands.has_any_role('Bot Whisperer')
async def RemBalOp(ctx, operationid):
    """To remove a balance operation.
       The command structure is `!RemBalOp <operationid>`
    """
    await ctx.message.delete()
    track_channel = get(ctx.guild.text_channels, id=840733014622601226)
    async with ctx.bot.mplus_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            query = """
                SELECT * FROM balance_ops where operation_id = %s
                    AND deleted_at IS NULL or deleted_at = ' '
            """
            val = (operationid,)
            await cursor.execute(query, val)
            notSoftDeleted =  await cursor.fetchone()
            if notSoftDeleted is not None:
                async with conn.cursor() as cursor:
                    query = """
                        UPDATE balance_ops SET 
                            DELETED_AT = UTC_TIMESTAMP()
                            WHERE operation_id = %s
                        """
                    val = {operationid,}
                    await cursor.execute(query, val)
                    em = discord.Embed(title="Balance Operation was Removed",
                                description=
                                    f"The operation with ID **{operationid}** was removed "
                                    f"by {ctx.message.author.mention}.",
                                color=discord.Color.orange())
                    await track_channel.send(embed=em)
                    await ctx.author.send(embed=em)
            else:
                await ctx.author.send(
                    f"The operation with ID {operationid} wasn't found in the Database or it was already removed.")


@bot.command()
@commands.has_any_role('Horde', 'Alliance')
async def Crossfaction(ctx, *, rio_link):
    """To add an user to crossfaction boosting.
       The command structure is !Crossfaction https://raider.io/characters/eu/tarren-mill/Sanfura
    """
    cross_channel = get(ctx.guild.text_channels, id=815104636251275306)
    if ctx.channel.id == 815104636251275306:
        global boosters
        await ctx.message.delete()
        bot_log_channel = get(ctx.guild.text_channels, name='bot-logs')
        
        raiderio_regex = re.compile(rio_conf.RAIDERIO_LINK)
        match = raiderio_regex.findall(rio_link)

        if not match:
            embed_bot_log = discord.Embed(
                title=f"{ctx.bot.user.name} Error Log.",
                description=f"on {ctx.command.name}",
                color=discord.Color.blue())
            embed_bot_log.add_field(name="Details:",
                                    value=f"Couldn't find regex match for {rio_link}")
            embed_bot_log.set_footer(text=datetime.now(timezone.utc).replace(microsecond=0))
            await bot_log_channel.send(embed=embed_bot_log)
            logger.error(f"========on {ctx.command.name} START=======")
            logger.error(f"Couldn't find regex match for {rio_link}")
            logger.error(f"========on {ctx.command.name} END=========")
            await ctx.author.send("""    
                    Wrong Raider.IO link, please double check it, 
                    example: https://raider.io/characters/eu/tarren-mill/Sanfura
                    """, delete_after=10)
            return

        realm = match[0][0]
        char = match[0][1]

        if not (realm and char):
            embed_bot_log = discord.Embed(
                title=f"{ctx.bot.user.name} Error Log.",
                description=f"on {ctx.command.name}",
                color=discord.Color.blue())
            embed_bot_log.add_field(name="Details:",
                                    value="Missing realm and character")
            embed_bot_log.set_footer(text=datetime.now(timezone.utc).replace(microsecond=0))
            logger.error(f"========on {ctx.command.name} START=======")
            logger.error("Missing realm and character")
            logger.error(f"========on {ctx.command.name} END=========")
            await bot_log_channel.send(embed=embed_bot_log)
            await ctx.send(
                "{ctx.author.mention} An error occurred, please DM "
                f"{get(ctx.guild.members, id=186433880872583169).mention}",delete_after=10)
            return

        HighKeyBoosterA = get(ctx.guild.roles, name='High Key Booster [A]')
        MBoosterA = get(ctx.guild.roles, name='M+ Booster [A]')
        HighKeyBoosterH = get(ctx.guild.roles, name='High Key Booster [H]')
        MBoosterH = get(ctx.guild.roles, name='M+ Booster [H]')
        RaiderH = get(ctx.guild.roles, name='Raider {H}')
        RaiderA = get(ctx.guild.roles, name='Raider {A}')
        RoleH = get(ctx.guild.roles, name='Horde')
        RoleA = get(ctx.guild.roles, name='Alliance')
        rio_url = (
            f"{rio_conf.base}/api/v1/characters/profile?region=eu"
            f"&realm={realm}"
            f"&name={char}&fields=mythic_plus_scores_by_season:current"
        )
        response = requests.get(rio_url)
        if response.status_code == 200:
            json_str = json.dumps(response.json())
            resp = json.loads(json_str)
            faction = resp["faction"]
            score = resp["mythic_plus_scores_by_season"][0]["scores"]["all"]
            if score < rio_conf.role_threshhold:
                await ctx.send(
                    f"{ctx.author.mention} the character has less than the required score {rio_conf.role_threshhold}")
                return
            else:
                faction_short = "H" if faction == "horde" else "A"
                realm_pre = realm.replace(' ', '').replace('-','').capitalize()
                if realm_pre.startswith("Pozzo"):
                    realm_final = "Pozzo"
                elif realm_pre == "Dunmodr":
                    realm_final = "DunModr"
                elif realm_pre.startswith("Twisting"):
                    realm_final = "TwistingNether"
                elif realm_pre.startswith("Tarren"):
                    realm_final = "TarrenMill"
                elif realm_pre == "Colinaspardas":
                    realm_final = "ColinasPardas"
                elif realm_pre == "Burninglegion":
                    realm_final = "BurningLegion"
                elif realm_pre == "Themaelstrom":
                    realm_final = "TheMaelstrom"
                elif realm_pre == "Defiasbrotherhood":
                    realm_final = "Defias"
                elif realm_pre == "Shatteredhand":
                    realm_final = "Shattered"
                elif realm_pre.startswith("Argent"):
                    realm_final = "ArgentDawn"
                elif realm_pre == "Burningblade":
                    realm_final = "BurningBlade"
                elif realm_pre.startswith("Aggra"):
                    realm_final = "Aggra"
                elif realm_pre.startswith("Chamberof"):
                    realm_final = "ChamberofAspects"
                elif realm_pre.startswith("Emerald"):
                    realm_final = "EmeraldDream"
                elif realm_pre.startswith("Grim"):
                    realm_final = "GrimBatol"
                elif realm_pre.startswith("Quel"):
                    realm_final = "Quel'Thalas"
                elif realm_pre.startswith("Mal'ganis"):
                    realm_final = "Mal'Ganis"
                elif realm_pre.startswith("Azjol"):
                    realm_final = "AzjolNerub"
                elif realm_pre.startswith("Los"):
                    realm_final = "LosErrantes"
                elif realm_pre.startswith("Twilight"):
                    realm_final = "Twilight'sHammer"
                else:
                    realm_final = realm_pre

                async with ctx.bot.ops_pool.acquire() as conn:
                    async with conn.cursor() as cursor:
                        query = """
                            SELECT * 
                            FROM cross_faction_boosters 
                            WHERE discord_id = %s
                        """
                        val = (ctx.author.id,)
                        await cursor.execute(query, val)
                        mainExists =  await cursor.fetchone()
                        if mainExists is None:
                            async with conn.cursor() as cursor:
                                query = """
                                    INSERT INTO cross_faction_boosters 
                                        (discord_id, alliance_name, horde_name) 
                                        VALUES (%s, %s, %s)
                                """
                                if ctx.author.nick.endswith("[H]"):
                                    alliance_name = f"{char}-{realm_final} [{faction_short}]"
                                    horde_name = ctx.author.nick
                                if ctx.author.nick.endswith("[A]"):
                                    alliance_name = ctx.author.nick
                                    horde_name = f"{char}-{realm_final} [{faction_short}]"
                                else:
                                    embed_bot_log = discord.Embed(
                                        title=f"{ctx.bot.user.name} Error Log.",
                                        description=f"on {ctx.command.name}",
                                        color=discord.Color.blue())
                                    embed_bot_log.add_field(name="Details:",
                                                            value="Missing realm and character")
                                    embed_bot_log.set_footer(text=datetime.now(timezone.utc).replace(microsecond=0))
                                    logger.error(f"========on {ctx.command.name} START=======")
                                    logger.error(f"Wrong author.nick format for {ctx.author.nick}")
                                    logger.error(f"========on {ctx.command.name} END=========")
                                    await bot_log_channel.send(embed=embed_bot_log)
                                    await ctx.send(
                                        "An error occurred, please DM "
                                        f"{get(ctx.guild.members, id=186433880872583169).mention}")
                                    return
                                val = (ctx.author.id, alliance_name, horde_name)
                                await cursor.execute(query, val)

                                if score >= 1600:
                                    if HighKeyBoosterA in ctx.author.roles:
                                        await ctx.author.add_roles(HighKeyBoosterH)
                                    if HighKeyBoosterH in ctx.author.roles:
                                        await ctx.author.add_roles(HighKeyBoosterA)

                                if MBoosterA in ctx.author.roles:
                                    await ctx.author.add_roles(MBoosterH)
                                if MBoosterH in ctx.author.roles:
                                    await ctx.author.add_roles(MBoosterA)
                                if RaiderH in ctx.author.roles:
                                    await ctx.author.add_roles(RaiderA)
                                if RaiderA in ctx.author.roles:
                                    await ctx.author.add_roles(RaiderH)
                                if RoleH in ctx.author.roles:
                                    await ctx.author.add_roles(RoleA)
                                if RoleA in ctx.author.roles:
                                    await ctx.author.add_roles(RoleH)
                                if ctx.author.nick.endswith("[H]"):
                                    em = discord.Embed(title="CrossFaction completed.",
                                        description=
                                            f"{ctx.message.author.mention} was added as a CrossFaction booster. "
                                            f"The character for crossfaction will be: {alliance_name}.",
                                        color=discord.Color.green())
                                    await ctx.author.send(embed=em)
                                    await cross_channel.send(embed=em, delete_after=10)
                                else:
                                    em = discord.Embed(title="CrossFaction completed.",
                                        description=
                                            f"{ctx.message.author.mention} was added as a CrossFaction booster. "
                                            f"The character for crossfaction will be: {horde_name}.",
                                        color=discord.Color.green())
                                    await ctx.author.send(embed=em)
                                    await cross_channel.send(embed=em, delete_after=10)
                                await cursor.execute("SELECT * FROM cross_faction_boosters")
                                boosters = await cursor.fetchall()
                        else:
                            em = discord.Embed(title="Already exists.",
                                description=
                                    f"{ctx.message.author.mention}, you are already signed "
                                    "up as a crossfaction booster. If you think this is an error, "
                                    f"please contact {get(ctx.guild.members, id=186433880872583169).mention}",
                                color=discord.Color.orange())
                            await ctx.author.send(embed=em)
                            await cross_channel.send(embed=em, delete_after=10)

        else:
            await ctx.send(f"{ctx.author.mention} An error occurred contacting raiderIO, "
                "please try again later", 
                delete_after=10)

@bot.command(aliases=['b', 'bal'])
async def balance_command(ctx, *, target_booster=None):
    """To Check booster balance.
    """
    await ctx.message.delete()
    if target_booster is None:
        name, realm = await checkPers(ctx.author.id)
        if name is None:
            name, realm = ctx.author.nick.split("-")

        balance_name = f"{name}-{realm}"
    else:
        Moderator_role = get(ctx.guild.roles, name="Moderator")
        Management_role = get(ctx.guild.roles, name="Management")
        if Moderator_role in ctx.author.roles or Management_role in ctx.author.roles:
            balance_name = target_booster
            ctx.command.reset_cooldown(ctx)
        else:
            return await ctx.send("You don't have permissions to check other members balance")

    balance_check_channel = get(ctx.guild.text_channels, id=815104636251275312)
    if ctx.message.channel.id != 815104636251275312:
        return await ctx.message.channel.send(
            f"Head to {balance_check_channel.mention} to issue the command", 
            delete_after=5)
    if not (balance_name.endswith("[A]") or balance_name.endswith("[H]")):
        return await ctx.send("Invalid name format")
    try:
        async with ctx.bot.mplus_pool.acquire() as conn:
            async with conn.cursor() as cursor:
                query = """
                    SELECT cur_balance, pre_balance, tot_balance 
                    FROM ov_creds 
                    WHERE booster=%s
                """
                val = (balance_name,)
                await cursor.execute(query, val)
                balance_result = await cursor.fetchall()
                if len(balance_result) > 0 :
                    cur_bal, pre_bal, tot_bal = balance_result
                else:
                    cur_bal = pre_bal = tot_bal = 0

                current_balance = f"🏧  {cur_bal:,}"
                previous_balance = f"🏧  {pre_bal:,}"
                total_balance = f"🏧  {tot_bal:,}"

                await ctx.send(f"{ctx.message.author.mention} balance has been sent in a DM", 
                                delete_after=3)
                balance_embed = discord.Embed(title="Balance Info!",
                                                description=f"{balance_name}",
                                                color=0xffd700)
                balance_embed.add_field(name="Current Balance",
                                        value=current_balance, inline=False)
                balance_embed.add_field(name="Previous Balance",
                                        value=previous_balance, inline=False)
                balance_embed.add_field(name="Total Balance",
                                        value=total_balance, inline=False)
                await ctx.author.send(embed=balance_embed)
    
    except discord.errors.Forbidden:
        await ctx.send(
            f"{ctx.message.author.mention} cannot send you a DM, please allow DM's from server members", 
            delete_after=5)


@bot.command()
@commands.has_any_role('Moderator', 'Staff', 'Management')
async def ExpCurCreds(ctx):
    """To Export current booster credits.
    """
    async with ctx.bot.mplus_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            exporting_msg = await ctx.send("Please hold... exporting")
            await cursor.execute("SELECT booster, cur_balance FROM ov_creds WHERE cur_balance <> 0")
            rows = await cursor.fetchall()
            data = [
                [
                    "Name",
                    "Balance"
                ]
            ]
            data += rows
            agc = await agcm.authorize()
            spreadsheet = await agc.open("BalanceCopyTarget")
            curWeek_sheet = await spreadsheet.worksheet("Current Week")
            await curWeek_sheet.clear()
            await curWeek_sheet.batch_update([{'range': 'A:B', 'values': data, }])
            spreadsheet_url = "https://docs.google.com/spreadsheets/d/%s" % spreadsheet.id
            await exporting_msg.delete()
            await ctx.send(f"Current Credits have been exported, you can find them here: {spreadsheet_url}")
    await ctx.message.delete()


@bot.command()
@commands.has_any_role('Moderator', 'Staff', 'Management')
async def ExpPreCreds(ctx):
    """To Export previous week booster credits.
    """
    async with ctx.bot.mplus_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            exporting_msg = await ctx.send("Please hold... exporting")
            await cursor.execute("SELECT booster, pre_balance FROM ov_creds WHERE cur_balance <> 0")
            rows = await cursor.fetchall()
            data = [
                [
                    "Name",
                    "Balance"
                ]
            ]
            data += rows
            agc = await agcm.authorize()
            spreadsheet = await agc.open("BalanceCopyTarget")
            preWeek_sheet = await spreadsheet.worksheet("Previous Week")
            await preWeek_sheet.clear()
            await preWeek_sheet.batch_update([{'range': 'A:B', 'values': data, }])
            spreadsheet_url = "https://docs.google.com/spreadsheets/d/%s" % spreadsheet.id
            await exporting_msg.delete()
            await ctx.send(f"Previous Credits have been exported, you can find them here: {spreadsheet_url}")
    await ctx.message.delete()


@bot.command()
@commands.has_any_role('Moderator', 'Staff', 'Management')
async def ExportStrikes(ctx):
    """To Export last week strikes.
    """
    await ctx.message.delete()
    async with ctx.bot.mplus_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            query = """
                SELECT author, COUNT(operation_id) FROM balance_ops WHERE deleted_at IS NULL AND 
                DATE(`date`) BETWEEN (SELECT pre1 FROM variables WHERE id = 1) AND 
                (SELECT pre2 FROM variables WHERE id = 1) AND command = 'Strike'
                GROUP BY author
            """
            await cursor.execute(query)
            rows = await cursor.fetchall()
            string_row = ""
            for row in rows:
                string_row +=f"{row[0]}: {row[1]}\n"

            await ctx.author.send(f"The number of strikes from the past week are: \n{string_row}")


@bot.command()
@commands.has_any_role('Moderator', 'Staff', 'Management')
async def ExportNegative(ctx):
    """To Export last week negative balance.
    """
    await ctx.message.delete()
    async with ctx.bot.mplus_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            query = """SELECT * FROM ov_creds WHERE balance LIKE '-%'
            """
            await cursor.execute(query)
            rows = await cursor.fetchall()
            string_row = ""
            for row in rows:
                string_row +=f"{row[0]}: {row[1]}\n"

            await ctx.author.send(f"People that is in debt with Nova: \n{string_row}")


@bot.command()
@commands.has_any_role('Collectors', 'Bot Whisperer')
async def Collections(ctx):
    """To check current and last week collections.
    """
    await ctx.message.delete()
    name, realm = await checkPers(ctx.author.id)
    if name is None:
        name, realm = ctx.author.nick.split("-")

    collector_name = f"{name}-{realm}"

    async with ctx.bot.mplus_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            query = "SET @collector := %s;"
            val = (collector_name,)
            await cursor.execute(query, val)
            query = """
                SELECT (SELECT COUNT(collection_id) FROM collectors WHERE collector = @collector AND deleted_at IS NULL AND 
                DATE(date_collected) BETWEEN (SELECT cur1 FROM variables WHERE id = 1) AND 
                (SELECT cur2 FROM variables WHERE id = 1)) AS Current_Week ,
                (SELECT COUNT(collection_id) FROM collectors WHERE collector = @collector AND deleted_at IS NULL AND
                DATE(date_collected) BETWEEN (SELECT pre1 FROM variables WHERE id = 1) AND 
                (SELECT pre2 FROM variables WHERE id = 1)) AS Previous_Week
            """
            await cursor.execute(query)
            rows = await cursor.fetchall()
            collections_embed = discord.Embed(title="Collections",
                                            description="Info!",
                                            color=0xffd700)
            collections_embed.add_field(name="Current Week",
                                    value=rows[0][0], inline=False)
            collections_embed.add_field(name="Previous Week",
                                    value=rows[0][1], inline=False)
            await ctx.author.send(embed=collections_embed)


# endregion

async def start_bot():
    mplus_pool = await aiomysql.create_pool(host=DB_HOST, port=3306,
                            user=DB_USER, password=DB_PASSWORD,
                            db=MPLUS_DB, autocommit=True)

    ops_pool = await aiomysql.create_pool(host=DB_HOST, port=3306,
                            user=DB_USER, password=DB_PASSWORD,
                            db=OPS_DB, autocommit=True)

    bot.mplus_pool = mplus_pool
    bot.ops_pool = ops_pool

    # bot.load_extension("cogs.moderation")
    # bot.load_extension("cogs.google_sheets")

    await bot.start(token)

try:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start_bot())
except Exception as e:
    logger.warning("Exception raised from main thread.")
    logger.exception(e)