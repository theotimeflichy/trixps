/**
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *  WARNING
 *
 *  THIS NEXT.JS HAS BEEN
 *  FULLY CODED BY CLAUDE
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */
"use client";

import { useCallback, useEffect, useRef, useState } from "react";

/**
 * This file is the whole tic-tac-toe app in a single page.
 *
 * Nothing is stored on a server: the game is event-sourced on the pub-sub log.
 * Matchmaking uses a shared "lobby" topic (heartbeats), and every move of a
 * game is published to a per-game topic then replayed to rebuild the board.
 */

// base url of the http/json gateway in front of the brokers
const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8080";

// lobby topic and its constant key (one partition => total order)
const LOBBY = "lobby";

// timings (ms)
const TICK_MS = 1000; // heartbeat + poll period in the waiting room
const POLL_MS = 700; // poll period during a game
const LIVE_NS = 3000 * 1e6; // a heartbeat older than this is considered dead

// shape of one record returned by the gateway
type Record = { offset: number; key: string; value: string; tsNano: number };

// a found match between two players
type Match = { gameId: string; mySymbol: "X" | "O"; partnerName: string };

// replayed state of a game
type GameState = {
  board: string[];
  winner: string | null;
  full: boolean;
  turn: "X" | "O";
};

/**
 * This function publish one message to a topic through the gateway
 *
 * @param topic the topic to publish to
 * @param key the message key (chooses the partition)
 * @param value the message payload
 */
async function publish(topic: string, key: string, value: string) {
  await fetch(`${API}/publish`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ topic, key, value }),
  });
}

/**
 * This function read messages of a topic from an offset through the gateway
 *
 * @param topic the topic to read from
 * @param key the message key (chooses the partition)
 * @param offset the offset to start reading from
 * @param max the maximum number of records to return
 * @return the records read
 */
async function consume(
  topic: string,
  key: string,
  offset: number,
  max: number
): Promise<Record[]> {
  const url = `${API}/consume?topic=${encodeURIComponent(
    topic
  )}&key=${encodeURIComponent(key)}&offset=${offset}&max=${max}`;
  const res = await fetch(url);
  if (!res.ok) return [];
  const data = await res.json();
  return data.records || [];
}

/**
 * This function find if the current player is paired with someone
 *
 * @param records the lobby records (heartbeats)
 * @param myId the current player id
 * @return the match if a partner is found, else null
 */
function findMatch(records: Record[], myId: string): Match | null {
  if (records.length === 0) return null;

  // newest server timestamp used as the "now" reference (avoids clock skew)
  let maxTs = 0;
  for (const r of records) if (r.tsNano > maxTs) maxTs = r.tsNano;

  // keep only the latest heartbeat of each still-alive player
  const latest = new Map<string, { id: string; name: string; ts: number }>();
  for (const r of records) {
    if (maxTs - r.tsNano > LIVE_NS) continue;
    let m: { playerId?: string; name?: string };
    try {
      m = JSON.parse(r.value);
    } catch {
      continue;
    }
    if (!m.playerId) continue;
    const prev = latest.get(m.playerId);
    if (!prev || r.tsNano >= prev.ts) {
      latest.set(m.playerId, { id: m.playerId, name: m.name || "?", ts: r.tsNano });
    }
  }

  // sort the alive players by id and pair them two by two
  const live = [...latest.values()].sort((a, b) => (a.id < b.id ? -1 : 1));
  const idx = live.findIndex((p) => p.id === myId);
  if (idx < 0) return null;

  // find my partner inside my pair
  const start = idx - (idx % 2);
  const partner = idx === start ? live[start + 1] : live[start];
  if (!partner) return null;

  // lower id is X and plays first
  const mySymbol = myId < partner.id ? "X" : "O";
  const gameId = [myId, partner.id].sort().join("_");
  return { gameId, mySymbol, partnerName: partner.name };
}

/**
 * This function rebuild a board by replaying the move log of a game
 *
 * @param records the move records in offset order
 * @return the current board, the winner, fullness and whose turn it is
 */
function replay(records: Record[]): GameState {
  const board = Array(9).fill("");
  let moves = 0;

  // apply each valid move (X first, alternating, never on a taken cell)
  for (const r of records) {
    let m: { symbol?: string; cell?: number };
    try {
      m = JSON.parse(r.value);
    } catch {
      continue;
    }
    const expected = moves % 2 === 0 ? "X" : "O";
    if (typeof m.cell !== "number" || m.cell < 0 || m.cell > 8) continue;
    if (board[m.cell] !== "") continue;
    if (m.symbol !== expected) continue;
    board[m.cell] = expected;
    moves++;
  }

  return {
    board,
    winner: winnerOf(board),
    full: board.every((c) => c !== ""),
    turn: moves % 2 === 0 ? "X" : "O",
  };
}

/**
 * This function return the winner symbol of a board if there is one
 *
 * @param board the 9 cells board
 * @return "X", "O" or null
 */
function winnerOf(board: string[]): string | null {
  const lines = [
    [0, 1, 2],
    [3, 4, 5],
    [6, 7, 8],
    [0, 3, 6],
    [1, 4, 7],
    [2, 5, 8],
    [0, 4, 8],
    [2, 4, 6],
  ];
  for (const [a, b, c] of lines) {
    if (board[a] && board[a] === board[b] && board[a] === board[c]) {
      return board[a];
    }
  }
  return null;
}

/**
 * Page is the single-page tic-tac-toe app.
 */
export default function Page() {
  // current phase of the app
  const [phase, setPhase] = useState<"name" | "waiting" | "game">("name");

  // identity of this tab (a fresh id is taken each time we enter the lobby)
  const [name, setName] = useState("");
  const [playerId, setPlayerId] = useState("");

  // current match and replayed game state
  const [match, setMatch] = useState<Match | null>(null);
  const [game, setGame] = useState<GameState | null>(null);

  // keep the latest playerId in a ref for the polling closures
  const idRef = useRef("");
  idRef.current = playerId;

  // console events shown at the bottom of the page (newest at the bottom)
  const [events, setEvents] = useState<
    { dir: "out" | "in"; line: string; ts: string }[]
  >([]);

  // the console is collapsed by default, toggled by its header
  const [showConsole, setShowConsole] = useState(false);

  // remember the highest offset already logged per topic (log each record once)
  const seenRef = useRef<Map<string, number>>(new Map());

  // ref to the console body to keep it scrolled to the bottom
  const logRef = useRef<HTMLDivElement>(null);

  /**
   * This function append one line to the console
   *
   * @param dir "out" for a publish, "in" for a received record
   * @param line the text to show
   */
  const pushEvent = useCallback((dir: "out" | "in", line: string) => {
    // shorten long payloads to keep the console readable
    const short = line.length > 90 ? line.slice(0, 90) + "…" : line;
    const ts = new Date().toLocaleTimeString();
    setEvents((prev) => [...prev, { dir, line: short, ts }].slice(-200));
  }, []);

  /**
   * This function log the new records of a topic (each offset only once)
   *
   * @param topic the topic the records come from
   * @param label the short label shown before the offset
   * @param records the records read from the topic
   */
  const logIncoming = useCallback(
    (topic: string, label: string, records: Record[]) => {
      // skip every offset we already printed for this topic
      const seen = seenRef.current.get(topic) ?? -1;
      let maxOff = seen;
      for (const r of records) {
        if (r.offset <= seen) continue;
        pushEvent("in", `${label}#${r.offset} ${r.value}`);
        if (r.offset > maxOff) maxOff = r.offset;
      }
      seenRef.current.set(topic, maxOff);
    },
    [pushEvent]
  );

  // keep the console scrolled to the bottom on new events (and when opened)
  useEffect(() => {
    const el = logRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [events, showConsole]);

  // prefill the name from a previous visit in this tab
  useEffect(() => {
    const saved = sessionStorage.getItem("name");
    if (saved) setName(saved);
  }, []);

  /**
   * This function enter the waiting room with a fresh player id
   */
  const enterLobby = useCallback(() => {
    // need a name to play
    if (!name.trim()) return;

    // take a fresh id so each match gets a brand new game id
    sessionStorage.setItem("name", name.trim());
    setPlayerId(crypto.randomUUID());
    setMatch(null);
    setGame(null);
    setPhase("waiting");
  }, [name]);

  // waiting room: heartbeat on the lobby and look for a partner
  useEffect(() => {
    if (phase !== "waiting" || !playerId) return;
    let active = true;

    async function tick() {
      // publish my heartbeat
      const beat = JSON.stringify({ playerId: idRef.current, name });
      await publish(LOBBY, LOBBY, beat);
      pushEvent("out", `PUB lobby ${beat}`);

      // read the lobby and try to find a pair
      const records = await consume(LOBBY, LOBBY, 0, 1000);
      if (!active) return;
      logIncoming(LOBBY, "lobby", records);
      const found = findMatch(records, idRef.current);
      if (found) {
        setMatch(found);
        setPhase("game");
      }
    }

    tick();
    const handle = setInterval(tick, TICK_MS);
    return () => {
      active = false;
      clearInterval(handle);
    };
  }, [phase, playerId, name, pushEvent, logIncoming]);

  // game topic derived from the match
  const gameTopic = match ? `game-${match.gameId}` : "";

  /**
   * This function reload the game by replaying its move log
   */
  const loadGame = useCallback(async () => {
    if (!match) return;

    // replay the move log into the current board
    const records = await consume(gameTopic, match.gameId, 0, 1000);
    logIncoming(gameTopic, "game", records);
    setGame(replay(records));
  }, [match, gameTopic, logIncoming]);

  // game: poll the move log and rebuild the board
  useEffect(() => {
    if (phase !== "game" || !match) return;
    let active = true;

    const tick = () => {
      if (active) loadGame();
    };
    tick();
    const handle = setInterval(tick, POLL_MS);
    return () => {
      active = false;
      clearInterval(handle);
    };
  }, [phase, match, loadGame]);

  /**
   * This function play a move on a cell if it is my turn
   *
   * @param cell the cell index (0..8)
   */
  const play = useCallback(
    async (cell: number) => {
      if (!match || !game) return;

      // only play on my turn, on an empty cell, before the game ends
      const myTurn = !game.winner && !game.full && game.turn === match.mySymbol;
      if (!myTurn || game.board[cell] !== "") return;

      // publish my move then refresh right away
      const mv = JSON.stringify({ by: playerId, symbol: match.mySymbol, cell });
      await publish(gameTopic, match.gameId, mv);
      pushEvent("out", `PUB game cell=${cell}`);
      loadGame();
    },
    [match, game, gameTopic, playerId, loadGame, pushEvent]
  );

  // --- build the content for the current phase ---

  let body;
  if (phase === "name") {
    // phase 1: ask for the name
    body = (
      <main className="container">
        <h1>Welcome</h1>
        <p className="muted">Enter your name to play tic-tac-toe online.</p>
        <input
          className="input"
          placeholder="Your name"
          value={name}
          onChange={(e) => setName(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && enterLobby()}
        />
        <br />
        <button className="btn" onClick={enterLobby} disabled={!name.trim()}>
          Enter waiting room
        </button>
      </main>
    );
  } else if (phase === "waiting") {
    // phase 2: wait for an opponent
    body = (
      <main className="container">
        <h1>Waiting room</h1>
        <p className="muted">Hi {name}, looking for an opponent…</p>
        <div className="spinner">● ● ●</div>
      </main>
    );
  } else {
    // phase 3: play the game
    const myTurn =
      game && match && !game.winner && !game.full && game.turn === match.mySymbol;

    // build the status line
    let status = "Loading…";
    if (game && match) {
      if (game.winner) {
        status = game.winner === match.mySymbol ? "You win!" : "You lose";
      } else if (game.full) {
        status = "Draw";
      } else {
        status = myTurn ? "Your turn" : `Waiting for ${match.partnerName}…`;
      }
    }

    body = (
      <main className="container">
        <h1>
          You are {match?.mySymbol} vs {match?.partnerName}
        </h1>
        <div className="status">{status}</div>

        <div className="board">
          {(game ? game.board : Array(9).fill("")).map((c, i) => (
            <button
              key={i}
              className="cell"
              onClick={() => play(i)}
              disabled={!myTurn || c !== ""}
            >
              {c}
            </button>
          ))}
        </div>

        {game && (game.winner || game.full) && (
          <button className="btn" onClick={enterLobby}>
            Play again
          </button>
        )}
      </main>
    );
  }

  // the phase content plus the live pub-sub console pinned at the bottom
  return (
    <>
      {body}

      {/* spacer reserving the room taken by the fixed console */}
      <div style={{ height: showConsole ? 250 : 38 }} />

      <section className="console">
        <button
          className="console-bar"
          onClick={() => setShowConsole((v) => !v)}
        >
          pub-sub traffic
          <span className="console-toggle">
            {showConsole ? "▾ hide" : "▸ show"}
          </span>
        </button>
        {showConsole && (
          <div className="console-body" ref={logRef}>
            {events.length === 0 ? (
              <div className="console-line">waiting for pub-sub traffic…</div>
            ) : (
              events.map((e, i) => (
                <div key={i} className={`console-line ${e.dir}`}>
                  <span className="console-ts">{e.ts}</span>{" "}
                  {e.dir === "out" ? "→" : "←"} {e.line}
                </div>
              ))
            )}
          </div>
        )}
      </section>
    </>
  );
}
