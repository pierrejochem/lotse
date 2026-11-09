/** A queue of one: `acquire` resolves in the order it was called, with the function that hands the turn on. */
export class Mutex {
  private tail: Promise<void> = Promise.resolve();

  /**
   * Waits for the turn. If `abort` settles first, the turn is handed on as soon as it comes and `acquire` rejects with
   * what `abort` rejected with.
   */
  async acquire(abort?: Promise<never>): Promise<() => void> {
    const prev = this.tail;
    let release!: () => void;
    const mine = new Promise<void>((resolve) => {
      release = resolve;
    });
    this.tail = prev.then(() => mine);
    if (abort === undefined) {
      await prev;
    } else {
      try {
        await Promise.race([prev, abort]);
      } catch (e) {
        void prev.then(release);
        throw e;
      }
    }
    return release;
  }
}
