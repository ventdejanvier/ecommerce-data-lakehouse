import { NextRequest, NextResponse } from 'next/server';

export async function POST(request: NextRequest) {
  try {
    const payload = await request.json();
    const backendBaseUrl = (process.env.BACKEND_API_URL ?? 'http://127.0.0.1:8000').replace(/\/$/, '');
    const backendResponse = await fetch(`${backendBaseUrl}/api/track`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
      cache: 'no-store',
    });

    const backendBody = await backendResponse.json().catch(() => ({}));

    if (!backendResponse.ok) {
      console.error('[DataLakehouse API] Backend tracking failed:', {
        status: backendResponse.status,
        body: backendBody,
      });

      return NextResponse.json(
        {
          success: false,
          forwarded: false,
          eventId: payload.eventId,
          backendStatus: backendResponse.status,
          backendBody,
        },
        { status: 502 }
      );
    }

    return NextResponse.json({ 
      success: true, 
      forwarded: true,
      eventId: backendBody.eventId ?? payload.eventId,
      backend: backendBody,
      receivedAt: new Date().toISOString()
    });
  } catch (error) {
    console.error('[DataLakehouse API] Error processing event:', error);
    return NextResponse.json(
      { success: false, error: 'Failed to process event' },
      { status: 400 }
    );
  }
}
