import { Test, TestingModule } from '@nestjs/testing';
import { AppController } from './app.controller';
import { AppService } from './app.service';

describe('AppController', () => {
  let appController: AppController;

  beforeEach(async () => {
    const app: TestingModule = await Test.createTestingModule({
      controllers: [AppController],
      providers: [
        {
          provide: AppService,
          useValue: {
            getStatus: () => ({
              service: 'nest-sse package is running',
              connections: 3,
            }),
            ensureClientId: (clientId: string) => clientId,
          },
        },
      ],
    }).compile();

    appController = app.get<AppController>(AppController);
  });

  describe('root', () => {
    it('should return service status and connection count', () => {
      expect(appController.getHello()).toEqual({
        service: 'nest-sse package is running',
        connections: 3,
      });
    });
  });
});
