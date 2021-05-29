from string import ascii_lowercase
import NOVA_EU

def convert_si_to_number(i):
    if not i:
        return 0

    total_stars = 0
    alpha = ascii_lowercase.replace("k", "").replace("m", "").replace("b", "")

    i = i.strip().replace(",", ".").lower()

    if not i or any(char in alpha for char in i):
        return total_stars

    if len(i) >= 1:
        if 'k' in i:
            total_stars = float(i.replace('k', '')) * 1000
        elif 'm' in i:
            total_stars = float(i.replace('m', '')) * 1000000
        elif 'b' in i:
            total_stars = float(i.replace('b', '')) * 1000000000
        else:
            total_stars = int(i)

    return int(total_stars)


async def search_nested_alliance(mylist, val):
    for i in range(len(mylist)):
        for j in range(len(mylist[i])):
            # print i,j
            if mylist[i][j] == val:
                return mylist[i][0]
    return None


async def search_nested_horde(mylist, val):
    for i in range(len(mylist)):
        for j in range(len(mylist[i])):
            # print i,j
            if mylist[i][j] == val:
                return mylist[i][1]
    return None


async def checkPers(id :int):
    async with NOVA_EU.bot.mplus_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            query = """
                SELECT name , serv FROM persdict WHERE discord_id = %s
            """
            val = (id,)
            await cursor.execute(query,val)
            result = await cursor.fetchone()
            if result is not None:
                name = result[0]
                realm = result[1]
            else:
                name = None
                realm = None
    return (name, realm)


async def get_embedded_fields(message=None, **kwargs):
    if not message:
        channel = kwargs.get('channel')
        message_id = kwargs.get('id')

        if channel and message_id:
            message = await channel.fetch_message(message_id)

    # If we could not retrieve a valid message, return
    if not message:
        return

    return message.embeds[0] and message.embeds[0].to_dict()['fields']


async def record_usage(ctx):
    async with ctx.bot.ops_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            query = """
                INSERT INTO commands_log (command_author, command_name, command_args, command_date) 
                VALUES (%s, %s, %s, %s)
            """
            if len(ctx.args[1:]) > 0 and ctx.args[0] is not None:
                val = (ctx.author.display_name, ctx.command.name, ', '.join(map(str,ctx.args[1:])), ctx.message.created_at.replace(microsecond=0))
                if len(ctx.kwargs) > 0:
                    y = list(val)
                    y[2] += " " + list(ctx.kwargs.values())[0]
                    val = tuple(y)
            else:
                val = (ctx.author.display_name, ctx.command.name, "no arguments passed", ctx.message.created_at.replace(microsecond=0))
            await cursor.execute(query, val)
