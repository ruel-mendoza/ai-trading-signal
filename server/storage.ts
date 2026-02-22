import { type Signal, type InsertSignal, signals } from "@shared/schema";
import { db } from "./db";
import { eq, desc } from "drizzle-orm";

export interface IStorage {
  getAllSignals(): Promise<Signal[]>;
  getSignalById(id: number): Promise<Signal | undefined>;
  getSignalsByCategory(category: string): Promise<Signal[]>;
  createSignal(signal: InsertSignal): Promise<Signal>;
  updateSignalStatus(id: number, status: string): Promise<Signal | undefined>;
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

  async createSignal(signal: InsertSignal): Promise<Signal> {
    const [created] = await db.insert(signals).values(signal).returning();
    return created;
  }

  async updateSignalStatus(id: number, status: string): Promise<Signal | undefined> {
    const [updated] = await db.update(signals).set({ status }).where(eq(signals.id, id)).returning();
    return updated;
  }
}

export const storage = new DatabaseStorage();
