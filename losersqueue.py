import pandas as pd
from riotwatcher import LolWatcher
import requests
import time

def getIds(apiKey, region, summonerName):
    response = requests.get('https://{}.api.riotgames.com/lol/summoner/v4/summoners/by-name/{}?api_key={}'.format(region, summonerName, apiKey))
    resJson = response.json()
    return resJson['puuid'], resJson['id']

def getNumSoloqGames(watcher, region, encSumId):
    rankedStats = watcher.league.by_summoner(region, encSumId)
    soloQueueStats = [q for q in rankedStats if q['queueType'] == 'RANKED_SOLO_5x5']
    soloQueueStats = soloQueueStats[0]
    return soloQueueStats['wins'] + soloQueueStats['losses']

# Note: max matches returned in one call is 100
def getMatches(watcher, region, puuid, startIndex, recordCount, queue):
    recordCount = min(recordCount, 100)
    matches = watcher.match.matchlist_by_puuid(region=region, puuid=puuid, start=startIndex, count=recordCount, queue=queue)
    return matches

def getAllRankedMatches(watcher, region, puuid, encSumId):
    numRankedGames = getNumSoloqGames(watcher, region, encSumId)
    rankedSolo = 420 # Ranked Solo 5v5, see https://static.developer.riotgames.com/docs/lol/queues.json
    allRankedMatches = []
    numCurrentRequests = 16
    numTwoMinuteRequests = 16
    while len(allRankedMatches) < numRankedGames:
        nextBatchSize = min((numRankedGames - len(allRankedMatches)), 100)
        lastMatchIndex = len(allRankedMatches)
        nextMatchBath = getMatches(watcher, region, puuid, lastMatchIndex, nextBatchSize, rankedSolo)
        numCurrentRequests += 1
        numTwoMinuteRequests += 1
        if numTwoMinuteRequests == 100:
            print('Current request limit met, waiting 2 minutes...')
            time.sleep(120)
            numCurrentRequests = 1
            numTwoMinuteRequests = 1
        elif numCurrentRequests == 20:
            print('Current request limit met, waiting 1 second...')
            time.sleep(1)
            numCurrentRequests = 1
        allRankedMatches.extend(nextMatchBath)
    time.sleep(120)
    return allRankedMatches

def unrollMatchDetails(matchDf, playerPuuid):
    unrolledDf = pd.DataFrame(index=range(0, len(matchDf.index)))
    unrolledDf['matchID'] = [md['matchId'] for md in matchDf['metadata']]
    unrolledDf['gameCreation'] = [md['gameCreation'] for md in matchDf['info']]
    unrolledDf['gameCreation'] = pd.to_datetime(unrolledDf['gameCreation'], unit='ms')
    unrolledDf['gameStartTimestamp'] = [md['gameStartTimestamp'] for md in matchDf['info']]
    unrolledDf['gameStartTimestamp'] = pd.to_datetime(unrolledDf['gameStartTimestamp'], unit='ms')
    unrolledDf['gameEndTimestamp'] = [md['gameEndTimestamp'] if 'gameEndTimestamp' in md else None for md in matchDf['info']]
    unrolledDf['gameEndTimestamp'] = pd.to_datetime(unrolledDf['gameEndTimestamp'], unit='ms')

    unrolledDf['gameId'] = [md['gameId'] for md in matchDf['info']]
    unrolledDf['gameVersion'] = [md['gameVersion'] for md in matchDf['info']]
    unrolledDf['gameDuration'] = [pd.Timedelta(value=md['gameDuration'],unit='sec') if 'gameEndTimestamp' in md else pd.Timedelta(value=md['gameDuration'],unit='milli') for md in matchDf['info']]
    participantsList = [x['participants'] for x in matchDf['info']]
    playerList = [list(filter(lambda x: x['puuid']==playerPuuid, parts))[0] for parts in participantsList]
    unrolledDf['playerWin'] = [p['win'] for p in playerList]
    # Add any other player stats you want in your dataFrame here
    # template: 
    # unrolledDf['colName'] = [p['participantDataKey'] for p in playerList]
    # check the kinds of player data you can return at https://developer.riotgames.com/apis#match-v5/GET_getMatch under "participants"


    streakLength = []
    streakType = []
    previousMatchResult = ''
    currentStreakType = ''
    currentStreakLength = 0
    for index, row in unrolledDf.iterrows():
        if row['playerWin']:
            currentStreakType = 'win'
        else:
            currentStreakType = 'lose'
        if previousMatchResult == currentStreakType:
            currentStreakLength += 1
        else:
            currentStreakLength = 1
        streakLength.append(currentStreakLength)
        streakType.append(currentStreakType)
        previousMatchResult = currentStreakType

    unrolledDf['streakLength'] = streakLength
    unrolledDf['streakLength'] = unrolledDf.shift(1)['streakLength']
    unrolledDf['streakType'] = streakType
    unrolledDf['streakType'] = unrolledDf.shift(1)['streakType']
    unrolledDf['timeSinceLastGame'] = unrolledDf.shift(1)['gameCreation'] - unrolledDf['gameEndTimestamp']

    return unrolledDf


if __name__ == "__main__":
    apiKey = 'YOUR_API_KEY_HERE'
    summonerName = 'SUMMONER_NAME_HERE'
    region = 'euw1'
    matchIdFile = 'matchIds.txt'
    matchDetailsFile = 'matchDetails.parquet'
    unrolledDfFile = 'unrolledDf.parquet'

    matchDetails = []
    puuid = ''
    encryptedSummonerId = ''

    watcher = LolWatcher(apiKey)
    puuid, encryptedSummonerId = getIds(apiKey, region, summonerName)
    allRankedIds = getAllRankedMatches(watcher, region, puuid, encryptedSummonerId)
    f = open(matchIdFile, "w")
    for matchId in allRankedIds:
        f.write(matchId + '\n')
    f.close()

    numCurrentRequests = 0
    numTwoMinuteRequests = 0
    
    for mID in allRankedIds:
        currentMatchDetail = watcher.match.by_id(region, mID)
        matchDetails.append(currentMatchDetail)
        numCurrentRequests += 1
        numTwoMinuteRequests += 1
        if numTwoMinuteRequests == 100:
            print('Current request limit met, waiting 2 minutes...')
            time.sleep(120)
            numCurrentRequests = 1
            numTwoMinuteRequests = 1
        elif numCurrentRequests == 20:
            print('Current request limit met, waiting 1 second...')
            time.sleep(1)
            numCurrentRequests = 1
    matchDetailDf = pd.DataFrame(data=matchDetails)
    matchDetailDf.to_parquet(matchDetailsFile)
    print('Participant puuid: {}'.format(puuid))
    print('Formatting DF into {}'.format(unrolledDfFile))
    unrolledDf = unrollMatchDetails(matchDetailDf, puuid)
    unrolledDf.to_parquet(unrolledDfFile, engine='fastparquet')