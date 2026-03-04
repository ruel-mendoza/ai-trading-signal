import { type Signal, type InsertSignal, signals } from "@shared/schema";
import { db } from "./db";
import { eq, desc, and } from "drizzle-orm";

export interface IStorage {
  getAllSignals(): Promise<Signal[]>;
  getSignalById(id: number): Promise<Signal | undefined>;
  getSignalsByCategory(category: string): Promise<Signal[]>;
  getActiveSignalByPair(pair: string): Promise<Signal | undefined>;
  createSignal(signal: InsertSignal): Promise<Signal>;
  updateSignalStatus(id: number, status: string): Promise<Signal | undefined>;
  expireActiveSignalsForPair(pair: string): Promise<number>;
}

export class DatabaseStorage implements IStorage {
  async getAllSignals(): Promise<Signal[]> {
    return db.select().from(signals).orderBy(desc(signals.createdAt));
  }

  async getSignalById(id: number): Promise<Signal | undefined> {
    const [signal] = await db.select().from(signals).where(eq(signals.id, id));
    return signal;
  }

  async getSignalsByCategory(category: string): Promise<Signal[]> {
    return db.select().from(signals).where(eq(signals.category, category)).orderBy(desc(signals.createdAt));
  }

  async getActiveSignalByPair(pair: string): Promise<Signal | undefined> {
    const [signal] = await db
      .select()
      .from(signals)
      .where(and(eq(signals.pair, pair), eq(signals.status, "active")))
      .orderBy(desc(signals.createdAt))
      .limit(1);
    return signal;
  }

  async createSignal(signal: InsertSignal): Promise<Signal> {
    const [created] = await db.insert(signals).values(signal).returning();
    return created;
  }

  async updateSignalStatus(id: number, status: string): Promise<Signal | undefined> {
    const [updated] = await db.update(signals).set({ status }).where(eq(signals.id, id)).returning();
    return updated;
  }

  async expireActiveSignalsForPair(pair: string): Promise<number> {
    const result = await db
      .update(signals)
      .set({ status: "expired" })
      .where(and(eq(signals.pair, pair), eq(signals.status, "active")))
      .returning();
    return result.length;
  }
}

export const storage = new DatabaseStorage();
