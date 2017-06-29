using System;
using System.Collections.Concurrent;
using System.Data;
using System.Linq;
using System.Threading;
using Kontur.GameStats.Server.Data.Core;
using Kontur.GameStats.Server.Data.Persistence;
using Kontur.GameStats.Server.Helpers;
using Kontur.GameStats.Server.Models;

namespace Kontur.GameStats.Server.StatisticsManagement
{
    public class StatisticsProcessor
    {
        private static Thread statisticsThread;

        private static bool isStarted;

        private static readonly BlockingCollection<int> MatchQueue = new BlockingCollection<int>();

        public static void AddMatchId(int id)
        {
            MatchQueue.Add(id);
        }

        private static void LoadNotProcessedMatchesFromDatabase()
        {
            using (var unitOfWork = new UnitOfWork())
            {
                foreach (var matchId in unitOfWork.Matches.GetNotProcessedMatchesIds())
                {
                    AddMatchId(matchId);
                }
            }
        }

        private static void UpdateServerStatistics(Match match, IUnitOfWork unitOfWork)
        {
            var server = match.Server;
            var serverStatistics = server.Statistics;

            serverStatistics.TotalMatchesPlayed++;
            serverStatistics.MaximumPopulation = Math.Max(serverStatistics.MaximumPopulation,
                match.Scoreboard.Count);

            serverStatistics.SumOfPopulations += match.Scoreboard.Count;

            if (serverStatistics.FirstMatchTimestamp == DateTime.MinValue)
                serverStatistics.FirstMatchTimestamp = match.Timestamp;

            var serverGameModeStats = unitOfWork.ServerGameModeStats.FindOrAddServerGameModeStats(server, match.GameMode);
            serverGameModeStats.MatchesPlayed++;
            unitOfWork.ServerGameModeStats.MarkAsModified(serverGameModeStats);

            var serverMapStats = unitOfWork.ServerMapStats.FindOrAddServerMapStats(server, match.Map);
            serverMapStats.MatchesPlayed++;
            unitOfWork.ServerMapStats.MarkAsModified(serverMapStats);

            int year, dayOfYear;
            TimeHelper.GetUtcYearAndDay(match.Timestamp, out year, out dayOfYear);

            var dateServerStats = unitOfWork.DateServerStats.FindOrAddDateServerStats(year, dayOfYear, server);
            dateServerStats.MatchesPlayed++;
            unitOfWork.DateServerStats.MarkAsModified(dateServerStats);

            unitOfWork.ServerStatistics.MarkAsModified(serverStatistics);
            
        }


        private static double ScoreboardPercent(int position, int totalPlayers)
        {
            if (totalPlayers == 1)
                return 100;

            return (double) (totalPlayers - position - 1) / (totalPlayers - 1) * 100;
        }

        private static bool CanBeBestPlayer(Player player)
        {
            var statistics = player.Statistics;
            return statistics.TotalMatchesPlayed >= BestPlayer.NeedTotalMatches && statistics.Deaths > 0;
        }


        private static void UpdatePlayersStatistics(Match match, IUnitOfWork unitOfWork)
        {
            var server = match.Server;
            var gameMode = match.GameMode;
            var scoreboard = match.Scoreboard.OrderBy(score => score.Position);

            foreach (var score in scoreboard)
            {
                var player = score.Player;
                var playerStatistics = player.Statistics;

                playerStatistics.TotalMatchesPlayed++;
                if (score.Position == 0) playerStatistics.TotalMatchesWon++;

                playerStatistics.SumOfScoreboardPercents += ScoreboardPercent(score.Position, scoreboard.Count());

                if (playerStatistics.FirstMatchTimestamp == DateTime.MinValue)
                    playerStatistics.FirstMatchTimestamp = match.Timestamp;

                playerStatistics.LastMatchTimestamp = match.Timestamp;

                playerStatistics.Kills += score.Kills;
                playerStatistics.Deaths += score.Deaths;

                if (CanBeBestPlayer(player))
                    unitOfWork.BestPlayers.FindOrAddBestPlayer(player);

                var playerServerStats = unitOfWork.PlayerServerStats.FindOrAddPlayerServerStats(player, server);
                playerServerStats.MatchesPlayed++;
                unitOfWork.PlayerServerStats.MarkAsModified(playerServerStats);

                var playerGameModeStats = unitOfWork.PlayerGameModeStats.FindOrAddPlayerGameModeStats(player, gameMode);
                playerGameModeStats.MatchesPlayed++;
                unitOfWork.PlayerGameModeStats.MarkAsModified(playerGameModeStats);

                int year, dayOfYear;
                TimeHelper.GetUtcYearAndDay(match.Timestamp, out year, out dayOfYear);

                var datePlayerStats = unitOfWork.DatePlayerStats.FindOrAddDatePlayerStats(year, dayOfYear, player);
                datePlayerStats.MatchesPlayed++;
                unitOfWork.DatePlayerStats.MarkAsModified(datePlayerStats);

                unitOfWork.PlayerStatistics.MarkAsModified(playerStatistics);
                
            }
        }


        private static void ProcessStatistics()
        {
            while (true)
            {
                var matchId = MatchQueue.Take();

                using (var unitOfWork = new UnitOfWork())
                {
                    try
                    {
                        var match = unitOfWork.Matches.FindById(matchId);
                        
                        if (match.IsProcessedForStatistics) continue;

                        UpdateServerStatistics(match, unitOfWork);
                        UpdatePlayersStatistics(match, unitOfWork);

                        match.IsProcessedForStatistics = true;
                        unitOfWork.Matches.MarkAsModified(match);

                        unitOfWork.Save();
                    }
                    catch (DataException)
                    {
                        MatchQueue.Add(matchId);
                    }
                }
            }
        }

        public static void Start()
        {
            if (isStarted) return;

            LoadNotProcessedMatchesFromDatabase();

            statisticsThread = new Thread(ProcessStatistics);
            statisticsThread.IsBackground = true;
            statisticsThread.Start();


            isStarted = true;
        }
    }
}