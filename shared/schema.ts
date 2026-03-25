import { sql } from "drizzle-orm";
import { pgTable, text, varchar, serial, integer, real, timestamp, boolean } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

export const users = pgTable("users", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  username: text("username").notNull().unique(),
  password: text("password").notNull(),
});

export const insertUserSchema = createInsertSchema(users).pick({
  username: true,
  password: true,
});

export type InsertUser = z.infer<typeof insertUserSchema>;
export type User = typeof users.$inferSelect;

export const signals = pgTable("signals", {
  id: serial("id").primaryKey(),
  pair: text("pair").notNull(),
  category: text("category").notNull(),
  direction: text("direction").notNull(),
  entryPrice: real("entry_price").notNull(),
  stopLoss: real("stop_loss").notNull(),
  takeProfit: real("take_profit").notNull(),
  status: text("status").notNull().default("active"),
  confidence: integer("confidence").notNull().default(75),
  analysis: text("analysis").notNull(),
  shortSummary: text("short_summary").notNull(),
  createdAt: timestamp("created_at").default(sql`CURRENT_TIMESTAMP`).notNull(),
  updatedAt: timestamp("updated_at").default(sql`CURRENT_TIMESTAMP`).notNull(),
});

export const insertSignalSchema = createInsertSchema(signals).omit({
  id: true,
  createdAt: true,
  updatedAt: true,
});

export type InsertSignal = z.infer<typeof insertSignalSchema>;
export type Signal = typeof signals.$inferSelect & {
  fullName?: string | null;
  source?: string;
  strategyName?: string;
};
